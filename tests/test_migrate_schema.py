"""Unit tests for SchemaExtractor with monkeypatched MariaDB adapter."""

from unittest.mock import MagicMock

from fuelrod_backup.migrate.schema import ColumnDef, SchemaExtractor, SchemaGenerator, TableDef


def _make_adapter(
    schemata_rows=None,
    table_rows=None,
    col_rows=None,
    idx_rows=None,
    fk_rows=None,
    view_rows=None,
    trigger_rows=None,
    routine_rows=None,
):
    adapter = MagicMock()
    adapter._query_rows.side_effect = lambda sql, params=(), **kw: {
        "SCHEMATA": schemata_rows or [{"DEFAULT_CHARACTER_SET_NAME": "utf8mb4", "DEFAULT_COLLATION_NAME": "utf8mb4_unicode_ci"}],
        "TABLES": table_rows or [],
        "COLUMNS": col_rows or [],
        "STATISTICS": idx_rows or [],
        "KEY_COLUMN_USAGE": fk_rows or [],
        "VIEWS": view_rows or [],
        "TRIGGERS": trigger_rows or [],
        "ROUTINES": routine_rows or [],
    }.get(next((k for k in ("SCHEMATA","TABLES","COLUMNS","STATISTICS","KEY_COLUMN_USAGE","VIEWS","TRIGGERS","ROUTINES") if k in sql.upper()), ""), [])
    return adapter


def _col_row(name, data_type="varchar", column_type="varchar(100)", nullable="YES",
             extra="", key="", comment="", char_len=100, precision=None, scale=None,
             default=None, gen_expr=None, ordinal=1):
    return {
        "COLUMN_NAME": name,
        "ORDINAL_POSITION": ordinal,
        "COLUMN_DEFAULT": default,
        "IS_NULLABLE": nullable,
        "DATA_TYPE": data_type,
        "CHARACTER_MAXIMUM_LENGTH": char_len,
        "NUMERIC_PRECISION": precision,
        "NUMERIC_SCALE": scale,
        "COLUMN_TYPE": column_type,
        "COLUMN_KEY": key,
        "EXTRA": extra,
        "COLUMN_COMMENT": comment,
        "GENERATION_EXPRESSION": gen_expr,
    }


class TestSchemaExtractor:
    def test_extract_returns_database_schema(self):
        adapter = _make_adapter(
            table_rows=[{"TABLE_NAME": "users", "TABLE_COMMENT": "", "AUTO_INCREMENT": None}],
            col_rows=[_col_row("id", "int", "int(11)", nullable="NO", key="PRI", extra="auto_increment", char_len=None)],
        )
        extractor = SchemaExtractor(adapter)
        schema = extractor.extract("mydb")
        assert schema.name == "mydb"
        assert len(schema.tables) == 1
        assert schema.tables[0].name == "users"

    def test_column_with_unsigned_detected(self):
        adapter = _make_adapter(
            table_rows=[{"TABLE_NAME": "t", "TABLE_COMMENT": "", "AUTO_INCREMENT": None}],
            col_rows=[_col_row("count", "int", "int(10) unsigned", char_len=None)],
        )
        extractor = SchemaExtractor(adapter)
        schema = extractor.extract("db")
        col = schema.tables[0].columns[0]
        assert col.is_unsigned is True

    def test_enum_values_extracted_from_column_type(self):
        adapter = _make_adapter(
            table_rows=[{"TABLE_NAME": "t", "TABLE_COMMENT": "", "AUTO_INCREMENT": None}],
            col_rows=[_col_row("status", "enum", "enum('active','inactive')", char_len=None)],
        )
        extractor = SchemaExtractor(adapter)
        schema = extractor.extract("db")
        col = schema.tables[0].columns[0]
        assert col.data_type == "enum"
        assert "active" in col.column_type

    def test_auto_increment_start_captured(self):
        adapter = _make_adapter(
            table_rows=[{"TABLE_NAME": "orders", "TABLE_COMMENT": "", "AUTO_INCREMENT": 1001}],
            col_rows=[_col_row("id", "int", "int(11)", nullable="NO", key="PRI", extra="auto_increment", char_len=None)],
        )
        extractor = SchemaExtractor(adapter)
        schema = extractor.extract("db")
        assert schema.tables[0].auto_increment_start == 1001

    def test_charset_extracted_from_schemata(self):
        adapter = _make_adapter(
            schemata_rows=[{"DEFAULT_CHARACTER_SET_NAME": "latin1", "DEFAULT_COLLATION_NAME": "latin1_swedish_ci"}],
        )
        extractor = SchemaExtractor(adapter)
        schema = extractor.extract("db")
        assert schema.charset == "latin1"

    def test_views_extracted(self):
        adapter = _make_adapter(
            view_rows=[{"TABLE_NAME": "v_users", "VIEW_DEFINITION": "SELECT * FROM users", "IS_UPDATABLE": "NO"}],
        )
        extractor = SchemaExtractor(adapter)
        schema = extractor.extract("db")
        assert len(schema.views) == 1
        assert schema.views[0].name == "v_users"

    def test_triggers_extracted(self):
        adapter = _make_adapter(
            trigger_rows=[{
                "TRIGGER_NAME": "before_insert_users",
                "EVENT_MANIPULATION": "INSERT",
                "EVENT_OBJECT_TABLE": "users",
                "ACTION_TIMING": "BEFORE",
                "ACTION_STATEMENT": "SET NEW.created_at = NOW()",
                "ACTION_ORIENTATION": "ROW",
                "DEFINER": "root@localhost",
            }],
        )
        extractor = SchemaExtractor(adapter)
        schema = extractor.extract("db")
        assert len(schema.triggers) == 1
        assert schema.triggers[0].timing == "BEFORE"


class TestSchemaGenerator:
    def _make_table(self, cols):
        return TableDef(name="users", columns=cols)

    def test_generated_ddl_contains_table_name(self):
        tdef = TableDef(name="users", columns=[
            ColumnDef("id", 1, "int", "int(11)", False, False, None, "auto_increment", "PRI", "", None, None, None, None),
        ])
        gen = SchemaGenerator(target_schema="public")
        result = gen.generate(
            __import__("fuelrod_backup.migrate.schema", fromlist=["DatabaseSchema"]).DatabaseSchema(
                name="mydb", tables=[tdef]
            )
        )
        assert any('"public"."users"' in s for s in result.pre_data)

    def test_identity_column_generated(self):
        from fuelrod_backup.migrate.schema import DatabaseSchema
        tdef = TableDef(name="t", columns=[
            ColumnDef("id", 1, "int", "int(11)", False, False, None, "auto_increment", "PRI", "", None, None, None, None),
        ])
        gen = SchemaGenerator()
        result = gen.generate(DatabaseSchema(name="db", tables=[tdef]))
        assert any("GENERATED ALWAYS AS IDENTITY" in s for s in result.pre_data)

    def test_primary_key_in_ddl(self):
        from fuelrod_backup.migrate.schema import DatabaseSchema
        tdef = TableDef(name="t", columns=[
            ColumnDef("id", 1, "int", "int(11)", False, False, None, "", "PRI", "", None, None, None, None),
            ColumnDef("name", 2, "varchar", "varchar(50)", True, False, None, "", "", "", 50, None, None, None),
        ])
        gen = SchemaGenerator()
        result = gen.generate(DatabaseSchema(name="db", tables=[tdef]))
        assert any('PRIMARY KEY' in s for s in result.pre_data)

    def test_enum_check_in_post_data(self):
        from fuelrod_backup.migrate.schema import DatabaseSchema
        tdef = TableDef(name="t", columns=[
            ColumnDef("status", 1, "enum", "enum('active','inactive')", False, False, None, "", "", "", None, None, None, None),
        ])
        gen = SchemaGenerator()
        result = gen.generate(DatabaseSchema(name="db", tables=[tdef]))
        assert any("CHECK" in s and "active" in s for s in result.post_data)

    def test_view_goes_to_unsupported(self):
        from fuelrod_backup.migrate.schema import DatabaseSchema, ViewDef
        gen = SchemaGenerator()
        result = gen.generate(DatabaseSchema(name="db", views=[
            ViewDef("v_users", "SELECT * FROM users", False)
        ]))
        assert result.unsupported
        assert "v_users" in result.unsupported[0]
