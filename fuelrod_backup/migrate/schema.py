"""Schema extraction from MariaDB information_schema and PostgreSQL DDL generation."""

from __future__ import annotations

from dataclasses import dataclass, field

from .transform import SqlTransformer, TransformResult
from .types import TypeMapper

# ──────────────────────────────────────────────────────────────────────────────
#  Domain dataclasses
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ColumnDef:
    name: str
    ordinal: int
    data_type: str
    column_type: str
    is_nullable: bool
    is_unsigned: bool
    default: str | None
    extra: str
    key: str
    comment: str
    character_maximum_length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    generation_expression: str | None


@dataclass
class IndexDef:
    name: str
    columns: list[str]
    is_unique: bool
    index_type: str


@dataclass
class ForeignKeyDef:
    name: str
    columns: list[str]
    ref_table: str
    ref_columns: list[str]
    on_update: str
    on_delete: str


@dataclass
class TableDef:
    name: str
    columns: list[ColumnDef] = field(default_factory=list)
    indexes: list[IndexDef] = field(default_factory=list)
    foreign_keys: list[ForeignKeyDef] = field(default_factory=list)
    auto_increment_start: int | None = None
    comment: str = ""


@dataclass
class ViewDef:
    name: str
    definition: str
    is_updatable: bool


@dataclass
class TriggerDef:
    name: str
    event: str
    table: str
    timing: str
    body: str
    orientation: str
    definer: str


@dataclass
class RoutineDef:
    name: str
    routine_type: str
    return_type: str
    body: str
    definer: str


@dataclass
class DatabaseSchema:
    name: str
    tables: list[TableDef] = field(default_factory=list)
    views: list[ViewDef] = field(default_factory=list)
    triggers: list[TriggerDef] = field(default_factory=list)
    routines: list[RoutineDef] = field(default_factory=list)
    charset: str = "utf8mb4"
    collation: str = "utf8mb4_unicode_ci"


@dataclass
class GeneratedDDL:
    pre_data: list[str] = field(default_factory=list)
    post_data: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    unsupported: list[str] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────────
#  SchemaExtractor
# ──────────────────────────────────────────────────────────────────────────────

class SchemaExtractor:
    """Reads MariaDB information_schema and builds a DatabaseSchema."""

    def __init__(self, adapter) -> None:
        self._adapter = adapter

    def extract(self, dbname: str) -> DatabaseSchema:
        schema = DatabaseSchema(name=dbname)

        # Charset and collation
        charset_rows = self._adapter._query_rows(
            "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME "
            "FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s",
            (dbname,),
        )
        if charset_rows:
            schema.charset = charset_rows[0]["DEFAULT_CHARACTER_SET_NAME"] or "utf8mb4"
            schema.collation = charset_rows[0]["DEFAULT_COLLATION_NAME"] or "utf8mb4_unicode_ci"

        # Tables
        table_rows = self._adapter._query_rows(
            "SELECT TABLE_NAME, TABLE_COMMENT, AUTO_INCREMENT "
            "FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME",
            (dbname,),
        )
        for tr in table_rows:
            tdef = TableDef(
                name=tr["TABLE_NAME"],
                comment=tr["TABLE_COMMENT"] or "",
                auto_increment_start=int(tr["AUTO_INCREMENT"]) if tr["AUTO_INCREMENT"] else None,
            )
            tdef.columns = self._extract_columns(dbname, tdef.name)
            tdef.indexes = self._extract_indexes(dbname, tdef.name)
            tdef.foreign_keys = self._extract_foreign_keys(dbname, tdef.name)
            schema.tables.append(tdef)

        # Views
        view_rows = self._adapter._query_rows(
            "SELECT TABLE_NAME, VIEW_DEFINITION, IS_UPDATABLE "
            "FROM information_schema.VIEWS "
            "WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME",
            (dbname,),
        )
        for vr in view_rows:
            schema.views.append(ViewDef(
                name=vr["TABLE_NAME"],
                definition=vr["VIEW_DEFINITION"] or "",
                is_updatable=(vr["IS_UPDATABLE"] == "YES"),
            ))

        # Triggers
        trigger_rows = self._adapter._query_rows(
            "SELECT TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, "
            "ACTION_TIMING, ACTION_STATEMENT, ACTION_ORIENTATION, DEFINER "
            "FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA = %s ORDER BY TRIGGER_NAME",
            (dbname,),
        )
        for tr in trigger_rows:
            schema.triggers.append(TriggerDef(
                name=tr["TRIGGER_NAME"],
                event=tr["EVENT_MANIPULATION"],
                table=tr["EVENT_OBJECT_TABLE"],
                timing=tr["ACTION_TIMING"],
                body=tr["ACTION_STATEMENT"] or "",
                orientation=tr["ACTION_ORIENTATION"],
                definer=tr["DEFINER"],
            ))

        # Routines
        routine_rows = self._adapter._query_rows(
            "SELECT ROUTINE_NAME, ROUTINE_TYPE, DATA_TYPE, ROUTINE_DEFINITION, DEFINER "
            "FROM information_schema.ROUTINES "
            "WHERE ROUTINE_SCHEMA = %s ORDER BY ROUTINE_TYPE, ROUTINE_NAME",
            (dbname,),
        )
        for rr in routine_rows:
            schema.routines.append(RoutineDef(
                name=rr["ROUTINE_NAME"],
                routine_type=rr["ROUTINE_TYPE"],
                return_type=rr["DATA_TYPE"] or "",
                body=rr["ROUTINE_DEFINITION"] or "",
                definer=rr["DEFINER"],
            ))

        return schema

    def _extract_columns(self, dbname: str, table_name: str) -> list[ColumnDef]:
        rows = self._adapter._query_rows(
            "SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, "
            "DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, "
            "COLUMN_TYPE, COLUMN_KEY, EXTRA, COLUMN_COMMENT, GENERATION_EXPRESSION "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            (dbname, table_name),
        )
        cols: list[ColumnDef] = []
        for r in rows:
            ct = (r["COLUMN_TYPE"] or "").lower()
            is_unsigned = "unsigned" in ct
            cols.append(ColumnDef(
                name=r["COLUMN_NAME"],
                ordinal=int(r["ORDINAL_POSITION"]),
                data_type=r["DATA_TYPE"] or "",
                column_type=r["COLUMN_TYPE"] or "",
                is_nullable=(r["IS_NULLABLE"] == "YES"),
                is_unsigned=is_unsigned,
                default=r["COLUMN_DEFAULT"],
                extra=r["EXTRA"] or "",
                key=r["COLUMN_KEY"] or "",
                comment=r["COLUMN_COMMENT"] or "",
                character_maximum_length=int(r["CHARACTER_MAXIMUM_LENGTH"]) if r["CHARACTER_MAXIMUM_LENGTH"] else None,
                numeric_precision=int(r["NUMERIC_PRECISION"]) if r["NUMERIC_PRECISION"] else None,
                numeric_scale=int(r["NUMERIC_SCALE"]) if r["NUMERIC_SCALE"] else None,
                generation_expression=r["GENERATION_EXPRESSION"],
            ))
        return cols

    def _extract_indexes(self, dbname: str, table_name: str) -> list[IndexDef]:
        rows = self._adapter._query_rows(
            "SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, INDEX_TYPE "
            "FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (dbname, table_name),
        )
        idx_map: dict[str, IndexDef] = {}
        for r in rows:
            name = r["INDEX_NAME"]
            if name == "PRIMARY":
                continue
            if name not in idx_map:
                idx_map[name] = IndexDef(
                    name=name,
                    columns=[],
                    is_unique=(str(r["NON_UNIQUE"]) == "0"),
                    index_type=r["INDEX_TYPE"] or "BTREE",
                )
            idx_map[name].columns.append(r["COLUMN_NAME"])
        return list(idx_map.values())

    def _extract_foreign_keys(self, dbname: str, table_name: str) -> list[ForeignKeyDef]:
        rows = self._adapter._query_rows(
            "SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, "
            "kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME, "
            "rc.UPDATE_RULE, rc.DELETE_RULE "
            "FROM information_schema.KEY_COLUMN_USAGE kcu "
            "JOIN information_schema.REFERENTIAL_CONSTRAINTS rc "
            "  ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
            "  AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA "
            "WHERE kcu.TABLE_SCHEMA = %s AND kcu.TABLE_NAME = %s "
            "ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
            (dbname, table_name),
        )
        fk_map: dict[str, ForeignKeyDef] = {}
        for r in rows:
            name = r["CONSTRAINT_NAME"]
            if name not in fk_map:
                fk_map[name] = ForeignKeyDef(
                    name=name,
                    columns=[],
                    ref_table=r["REFERENCED_TABLE_NAME"],
                    ref_columns=[],
                    on_update=r["UPDATE_RULE"] or "NO ACTION",
                    on_delete=r["DELETE_RULE"] or "NO ACTION",
                )
            fk_map[name].columns.append(r["COLUMN_NAME"])
            fk_map[name].ref_columns.append(r["REFERENCED_COLUMN_NAME"])
        return list(fk_map.values())


_IDENTITY_COMPATIBLE = frozenset({"SMALLINT", "INTEGER", "BIGINT"})


def _coerce_identity_type(pg_type: str, col: ColumnDef, warnings: list[str]) -> str:
    """Return a GENERATED ALWAYS AS IDENTITY-compatible type for *col*.

    PostgreSQL only allows SMALLINT, INTEGER, or BIGINT as identity columns.
    If the mapper returned something else (e.g. NUMERIC(20,0) for BIGINT UNSIGNED,
    or BOOLEAN for TINYINT(1)), promote to the smallest fitting integer type.
    """
    base = pg_type.upper().split("(")[0].strip()
    if base in _IDENTITY_COMPATIBLE:
        return pg_type

    # NUMERIC(20,0) from BIGINT UNSIGNED — use BIGINT; values above 9223372036854775807
    # are astronomically unlikely in an auto-increment PK
    if "NUMERIC" in base or "DECIMAL" in base:
        warnings.append(
            f"Identity column '{col.name}' had type {pg_type} (from BIGINT UNSIGNED) "
            f"— coerced to BIGINT for GENERATED ALWAYS AS IDENTITY compatibility."
        )
        return "BIGINT"

    # BOOLEAN from TINYINT(1) — extremely unusual for a PK but handle it
    if base == "BOOLEAN":
        warnings.append(
            f"Identity column '{col.name}' had type BOOLEAN (from TINYINT(1)) "
            f"— coerced to SMALLINT for GENERATED ALWAYS AS IDENTITY compatibility."
        )
        return "SMALLINT"

    # REAL, DOUBLE PRECISION, TEXT, etc. — promote to BIGINT and warn
    warnings.append(
        f"Identity column '{col.name}' had non-integer type {pg_type} "
        f"— coerced to BIGINT for GENERATED ALWAYS AS IDENTITY compatibility."
    )
    return "BIGINT"


# ──────────────────────────────────────────────────────────────────────────────
#  SchemaGenerator
# ──────────────────────────────────────────────────────────────────────────────

class SchemaGenerator:
    """Generates PostgreSQL DDL from a DatabaseSchema."""

    def __init__(
        self,
        *,
        target_schema: str = "public",
        unsigned_checks: bool = False,
        enum_as_type: bool = False,
    ) -> None:
        self._target_schema = target_schema
        self._unsigned_checks = unsigned_checks
        self._enum_as_type = enum_as_type
        self._mapper = TypeMapper()
        self._transformer = SqlTransformer()

    def generate(self, schema: DatabaseSchema) -> GeneratedDDL:
        result = GeneratedDDL()

        for table in schema.tables:
            table_ddl = self._generate_table(table)
            result.pre_data.append(table_ddl.ddl)
            result.post_data.extend(table_ddl.post_ddl)
            result.warnings.extend(table_ddl.warnings)

        for view in schema.views:
            result.unsupported.append(self._format_view(view))

        for trigger in schema.triggers:
            result.unsupported.append(self._format_trigger(trigger))

        for routine in schema.routines:
            result.unsupported.append(self._format_routine(routine))

        return result

    def _generate_table(self, table: TableDef) -> TransformResult:
        sc = self._target_schema
        col_defs: list[str] = []
        post: list[str] = []
        warnings: list[str] = []

        pk_cols = [c.name for c in table.columns if c.key == "PRI"]

        for col in table.columns:
            pg_type, type_warnings = self._mapper.map(
                data_type=col.data_type,
                column_type=col.column_type,
                is_unsigned=col.is_unsigned,
                extra=col.extra,
                character_maximum_length=col.character_maximum_length,
                numeric_precision=col.numeric_precision,
                numeric_scale=col.numeric_scale,
            )
            warnings.extend(type_warnings)

            is_identity = "auto_increment" in col.extra.lower()

            # ENUM → TEXT + CHECK
            if col.data_type.lower() == "enum":
                import re
                values = re.findall(r"'([^']*)'", col.column_type)
                check_values = ", ".join(f"'{v}'" for v in values)
                post.append(
                    f'ALTER TABLE "{sc}"."{table.name}" '
                    f'ADD CONSTRAINT "{table.name}_{col.name}_chk" '
                    f'CHECK ("{col.name}" IN ({check_values}));'
                )

            nullable = "" if col.is_nullable else " NOT NULL"

            if is_identity:
                identity_type = _coerce_identity_type(pg_type, col, warnings)
                col_defs.append(f'    "{col.name}" {identity_type} GENERATED ALWAYS AS IDENTITY{nullable}')
            else:
                default_clause = self._render_default(col, pg_type)
                col_defs.append(f'    "{col.name}" {pg_type}{nullable}{default_clause}')

            if col.comment:
                post.append(
                    f'COMMENT ON COLUMN "{sc}"."{table.name}"."{col.name}" '
                    f"IS '{col.comment.replace(chr(39), chr(39)*2)}';"
                )

        if pk_cols:
            pk_list = ", ".join(f'"{c}"' for c in pk_cols)
            col_defs.append(f"    PRIMARY KEY ({pk_list})")

        ddl = (
            f'CREATE TABLE IF NOT EXISTS "{sc}"."{table.name}" (\n'
            + ",\n".join(col_defs)
            + "\n);"
        )

        # Indexes (non-PK, non-FK)
        for idx in table.indexes:
            col_list = ", ".join(f'"{c}"' for c in idx.columns)
            if idx.index_type == "FULLTEXT":
                warnings.append(
                    f"FULLTEXT index '{idx.name}' on '{table.name}' requires manual migration. "
                    f"Suggested: CREATE INDEX using gin(to_tsvector(...))."
                )
                post.append(
                    f'-- FULLTEXT index "{idx.name}" — review manually:\n'
                    f'-- CREATE INDEX "{idx.name}_fts" ON "{sc}"."{table.name}" '
                    f'USING gin(to_tsvector(\'english\', {idx.columns[0]}));'
                )
            elif idx.is_unique:
                post.append(
                    f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx.name}" '
                    f'ON "{sc}"."{table.name}" ({col_list});'
                )
            else:
                post.append(
                    f'CREATE INDEX IF NOT EXISTS "{idx.name}" '
                    f'ON "{sc}"."{table.name}" ({col_list});'
                )

        # Foreign keys
        for fk in table.foreign_keys:
            src_cols = ", ".join(f'"{c}"' for c in fk.columns)
            ref_cols = ", ".join(f'"{c}"' for c in fk.ref_columns)
            post.append(
                f'ALTER TABLE "{sc}"."{table.name}" '
                f'ADD CONSTRAINT "{fk.name}" '
                f'FOREIGN KEY ({src_cols}) '
                f'REFERENCES "{sc}"."{fk.ref_table}" ({ref_cols}) '
                f'ON DELETE {fk.on_delete} ON UPDATE {fk.on_update} '
                f'DEFERRABLE INITIALLY DEFERRED;'
            )

        # Table comment
        if table.comment:
            post.append(
                f'COMMENT ON TABLE "{sc}"."{table.name}" '
                f"IS '{table.comment.replace(chr(39), chr(39)*2)}';"
            )

        return TransformResult(ddl=ddl, post_ddl=post, warnings=warnings)

    @staticmethod
    def _render_default(col: ColumnDef, pg_type: str) -> str:
        if col.default is None:
            return ""
        d = col.default

        # Normalise: strip outer parentheses that MariaDB sometimes adds,
        # e.g. "(CURRENT_TIMESTAMP)" or "(0)" stored in information_schema.
        stripped = d.strip()
        if stripped.startswith("(") and stripped.endswith(")"):
            stripped = stripped[1:-1].strip()

        upper = stripped.upper()

        # ── SQL function defaults — emit as bare keywords, never as string literals ──
        # MariaDB stores these with or without "()" so check both forms.
        _TS_FUNCS = {
            "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()",
            "NOW()", "NOW",
            "LOCALTIME", "LOCALTIME()",
            "LOCALTIMESTAMP", "LOCALTIMESTAMP()",
        }
        _DATE_FUNCS = {"CURRENT_DATE", "CURRENT_DATE()"}
        _TIME_FUNCS = {"CURRENT_TIME", "CURRENT_TIME()"}

        if upper in _TS_FUNCS:
            return " DEFAULT CURRENT_TIMESTAMP"
        if upper in _DATE_FUNCS:
            return " DEFAULT CURRENT_DATE"
        if upper in _TIME_FUNCS:
            return " DEFAULT CURRENT_TIME"
        if upper == "NULL":
            return " DEFAULT NULL"

        # ── Boolean coercion ──────────────────────────────────────────────────
        if pg_type == "BOOLEAN":
            if stripped in ("1", "TRUE", "true"):
                return " DEFAULT TRUE"
            if stripped in ("0", "FALSE", "false"):
                return " DEFAULT FALSE"

        # ── BIT literal: b'0' → false, b'1' → true (BIT columns map to BOOLEAN)
        if stripped.startswith("b'") and stripped.endswith("'"):
            bits = stripped[2:-1]
            return f" DEFAULT {'true' if int(bits or '0', 2) else 'false'}"

        # ── Numeric literals — emit unquoted ──────────────────────────────────
        try:
            float(stripped)
            return f" DEFAULT {stripped}"
        except ValueError:
            pass

        # ── Anything else — emit as a quoted string literal ───────────────────
        safe = stripped.replace("'", "''")
        return f" DEFAULT '{safe}'"

    @staticmethod
    def _format_view(view: ViewDef) -> str:
        return (
            f"-- VIEW: {view.name} (requires manual conversion)\n"
            f"-- Original MariaDB definition:\n"
            + "\n".join(f"-- {line}" for line in view.definition.splitlines())
            + "\n"
        )

    @staticmethod
    def _format_trigger(trigger: TriggerDef) -> str:
        return (
            f"-- TRIGGER: {trigger.name} ON {trigger.table} "
            f"({trigger.timing} {trigger.event}) — requires PL/pgSQL conversion\n"
            + "\n".join(f"-- {line}" for line in trigger.body.splitlines())
            + "\n"
        )

    @staticmethod
    def _format_routine(routine: RoutineDef) -> str:
        return (
            f"-- {routine.routine_type}: {routine.name} — requires PL/pgSQL conversion\n"
            + "\n".join(f"-- {line}" for line in routine.body.splitlines())
            + "\n"
        )
