"""Tests for pg_restore TOC parsing helpers in fuelrod_backup/restore.py."""


from fuelrod_backup.restore import (
    _iter_toc,
    _parse_owners_from_toc,
    _parse_schemas_from_toc,
    _parse_tables_from_toc,
    _split_toc_line,
)

# ---------------------------------------------------------------------------
# Realistic sample TOC — mirrors pg_restore --list output format:
#   <id>; <oid> <oid> <TYPE> [subtype] <SCHEMA> <NAME> <OWNER>
# ---------------------------------------------------------------------------
SAMPLE_TOC = """\
; Archive created at 2024-01-01 00:00:00 UTC
;     dbname: testdb
;
1; 2615 100 SCHEMA - public postgres
2; 2615 101 SCHEMA - app_schema appuser
3; 2615 102 SCHEMA - information_schema postgres
4; 2615 103 SCHEMA - pg_catalog postgres
5; 1259 200 TABLE public orders appuser
6; 1259 201 TABLE app_schema products appuser
7; 1259 202 TABLE public customers admin_user
8; 0 200 TABLE DATA public orders appuser
9; 0 0 FK CONSTRAINT public orders orders_pkey appuser
10; 0 0 SEQUENCE public orders_id_seq appuser
11; 0 0 SEQUENCE OWNED BY public orders_id_seq appuser
12; 0 0 ROLE - appuser appuser
"""


# ──────────────────────────────────────────────────────────────────────────────
#  _split_toc_line
# ──────────────────────────────────────────────────────────────────────────────

class TestSplitTocLine:
    def test_simple_table(self):
        parts = ["5;", "1259", "200", "TABLE", "public", "orders", "appuser"]
        assert _split_toc_line(parts) == ("TABLE", "public", "orders", "appuser")

    def test_compound_table_data(self):
        parts = ["8;", "0", "200", "TABLE", "DATA", "public", "orders", "appuser"]
        assert _split_toc_line(parts) == ("TABLE DATA", "public", "orders", "appuser")

    def test_compound_fk_constraint(self):
        # Simplified format (no table name) — still works because owner=parts[-1].
        parts = ["9;", "0", "0", "FK", "CONSTRAINT", "public", "orders_pkey", "appuser"]
        assert _split_toc_line(parts) == ("FK CONSTRAINT", "public", "orders_pkey", "appuser")

    def test_compound_sequence_owned_by(self):
        parts = ["11;", "0", "0", "SEQUENCE", "OWNED", "BY", "public", "orders_id_seq", "appuser"]
        assert _split_toc_line(parts) == ("SEQUENCE OWNED BY", "public", "orders_id_seq", "appuser")

    def test_compound_default_acl(self):
        parts = ["20;", "0", "0", "DEFAULT", "ACL", "public", "myacl", "appuser"]
        assert _split_toc_line(parts) == ("DEFAULT ACL", "public", "myacl", "appuser")

    def test_schema_object(self):
        parts = ["1;", "2615", "100", "SCHEMA", "-", "public", "postgres"]
        assert _split_toc_line(parts) == ("SCHEMA", "-", "public", "postgres")

    def test_too_few_parts_returns_none(self):
        assert _split_toc_line(["1;", "0", "0", "TABLE", "public"]) is None

    def test_six_parts_returns_none(self):
        # 6 parts lacks a separate owner token — below the 7-token minimum.
        assert _split_toc_line(["1;", "0", "0", "TABLE", "public", "orders"]) is None

    def test_fk_constraint_with_table_name(self):
        # Real pg_restore format includes the table name between schema and constraint name.
        # Previously "orders_pkey" landed in the owner slot; real owner "appuser" was lost.
        parts = ["9;", "0", "0", "FK", "CONSTRAINT", "public", "orders", "orders_pkey", "appuser"]
        assert _split_toc_line(parts) == ("FK CONSTRAINT", "public", "orders_pkey", "appuser")

    def test_constraint_with_table_name(self):
        # PRIMARY KEY / CHECK constraints also embed the table name in the tag.
        parts = ["9;", "2606", "12346", "CONSTRAINT", "public", "orders", "orders_pkey", "appuser"]
        assert _split_toc_line(parts) == ("CONSTRAINT", "public", "orders_pkey", "appuser")


# ──────────────────────────────────────────────────────────────────────────────
#  _iter_toc
# ──────────────────────────────────────────────────────────────────────────────

class TestIterToc:
    def test_skips_comment_lines(self):
        toc = "; this is a comment\n5; 1259 200 TABLE public orders appuser\n"
        entries = list(_iter_toc(toc))
        assert len(entries) == 1
        assert entries[0][0] == "TABLE"

    def test_skips_blank_lines(self):
        toc = "\n\n5; 1259 200 TABLE public orders appuser\n\n"
        entries = list(_iter_toc(toc))
        assert len(entries) == 1

    def test_empty_toc(self):
        assert list(_iter_toc("")) == []

    def test_all_comments(self):
        toc = "; line 1\n; line 2\n; line 3\n"
        assert list(_iter_toc(toc)) == []

    def test_yields_correct_tuple_structure(self):
        toc = "5; 1259 200 TABLE public orders appuser\n"
        (obj_type, schema, name, owner) = next(_iter_toc(toc))
        assert obj_type == "TABLE"
        assert schema == "public"
        assert name == "orders"
        assert owner == "appuser"


# ──────────────────────────────────────────────────────────────────────────────
#  _parse_schemas_from_toc
# ──────────────────────────────────────────────────────────────────────────────

class TestParseSchemas:
    def test_extracts_user_schemas(self):
        schemas = _parse_schemas_from_toc(SAMPLE_TOC)
        assert "public" in schemas
        assert "app_schema" in schemas

    def test_excludes_system_schemas(self):
        schemas = _parse_schemas_from_toc(SAMPLE_TOC)
        assert "information_schema" not in schemas
        assert "pg_catalog" not in schemas

    def test_result_is_sorted(self):
        schemas = _parse_schemas_from_toc(SAMPLE_TOC)
        assert schemas == sorted(schemas)

    def test_empty_toc(self):
        assert _parse_schemas_from_toc("") == []

    def test_schema_from_object_schema_column(self):
        # Table in app_schema should surface app_schema even without a SCHEMA entry.
        toc = "5; 1259 200 TABLE app_schema orders appuser\n"
        assert "app_schema" in _parse_schemas_from_toc(toc)

    def test_dash_placeholder_excluded(self):
        # The schema column is "-" for SCHEMA objects (name holds the schema).
        # "-" should never appear in the result.
        schemas = _parse_schemas_from_toc(SAMPLE_TOC)
        assert "-" not in schemas

    def test_pg_prefix_schemas_excluded(self):
        toc = "1; 2615 100 SCHEMA - pg_toast postgres\n"
        assert _parse_schemas_from_toc(toc) == []


# ──────────────────────────────────────────────────────────────────────────────
#  _parse_owners_from_toc
# ──────────────────────────────────────────────────────────────────────────────

class TestParseOwners:
    def test_extracts_non_system_owners(self):
        owners = _parse_owners_from_toc(SAMPLE_TOC)
        assert "appuser" in owners
        assert "admin_user" in owners

    def test_excludes_postgres_role(self):
        owners = _parse_owners_from_toc(SAMPLE_TOC)
        assert "postgres" not in owners

    def test_excludes_pg_prefixed_roles(self):
        toc = "5; 1259 200 TABLE public orders pg_monitor\n"
        assert _parse_owners_from_toc(toc) == []

    def test_explicit_role_object(self):
        toc = "12; 0 0 ROLE - appuser appuser\n"
        owners = _parse_owners_from_toc(toc)
        assert "appuser" in owners

    def test_no_duplicates(self):
        toc = (
            "5; 1259 200 TABLE public orders appuser\n"
            "6; 1259 201 TABLE public products appuser\n"
        )
        owners = _parse_owners_from_toc(toc)
        assert owners.count("appuser") == 1

    def test_result_is_sorted(self):
        owners = _parse_owners_from_toc(SAMPLE_TOC)
        assert owners == sorted(owners)

    def test_empty_toc(self):
        assert _parse_owners_from_toc("") == []

    def test_compound_type_owner_extracted_correctly(self):
        # TABLE DATA line — owner is always parts[-1].
        toc = "8; 0 200 TABLE DATA public orders realowner\n"
        owners = _parse_owners_from_toc(toc)
        assert "realowner" in owners


# ──────────────────────────────────────────────────────────────────────────────
#  _parse_tables_from_toc
# ──────────────────────────────────────────────────────────────────────────────

class TestParseTables:
    def test_tables_in_selected_schema(self):
        tables = _parse_tables_from_toc(SAMPLE_TOC, ["public"])
        assert "public.orders" in tables
        assert "public.customers" in tables

    def test_tables_in_other_schema_excluded(self):
        tables = _parse_tables_from_toc(SAMPLE_TOC, ["public"])
        assert "app_schema.products" not in tables

    def test_non_table_objects_excluded(self):
        tables = _parse_tables_from_toc(SAMPLE_TOC, ["public"])
        # Sequences, constraints, schema objects should not appear.
        assert all("." in t and t.split(".")[0] == "public" for t in tables)
        # TABLE DATA entries are compound — obj_type is "TABLE DATA", not "TABLE".
        assert "public.orders" in tables  # from TABLE entry
        count = sum(1 for t in tables if t == "public.orders")
        assert count == 1  # only from the TABLE line, not the TABLE DATA line

    def test_empty_schema_list_returns_empty(self):
        assert _parse_tables_from_toc(SAMPLE_TOC, []) == []

    def test_empty_toc(self):
        assert _parse_tables_from_toc("", ["public"]) == []
