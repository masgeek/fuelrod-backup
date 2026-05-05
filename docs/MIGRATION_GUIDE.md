# MariaDB → PostgreSQL Migration Utility — Implementation Plan

## Context

`fuelrod-backup` already manages backup/restore for MariaDB and PostgreSQL via the adapter pattern.
The missing capability is *cross-engine migration*: reading live data from MariaDB and writing it
into PostgreSQL with schema conversion, type mapping, validation, and a structured report.
This plan adds a `migrate` CLI command that integrates naturally with the existing config, adapter,
wizard, and test patterns — no new frameworks, no new config format.

---

## Objectives and Scope

**In scope:**
- Schema extraction from MariaDB `information_schema`
- DDL transformation (type mapping, syntax rewriting) to PostgreSQL
- Streaming row-batch data migration (MariaDB → PostgreSQL)
- Post-migration validation (row counts + optional MD5 checksums)
- JSON migration report (per-table status, warnings, skipped rows)
- Interactive wizard (same UX as `backup.py`) and non-interactive `--no-interactive` mode
- Dry-run mode (plan only, no writes)

**Out of scope (flagged, not migrated):**
- Stored procedures / functions (extracted to `<db>_routines_mariadb.sql`, manual conversion required)
- Triggers (extracted to `<db>_triggers_mariadb.sql`, manual conversion required)
- Views (commented-out DDL in report; MySQL-specific functions must be rewritten manually)
- Spatial types (warn; PostGIS setup is user responsibility)
- Application code changes

---

## Module Structure

New sub-package: `fuelrod_backup/migrate/`

```
fuelrod_backup/migrate/
    __init__.py        # exports run_migrate(), MigrateError
    runner.py          # MigrateRunner — top-level pipeline orchestrator
    schema.py          # SchemaExtractor (information_schema SQL) + SchemaGenerator (PG DDL)
    types.py           # TypeMapper — stateless MariaDB → PG type conversion
    transform.py       # SqlTransformer — DDL rewriting rules, returns TransformResult
    data.py            # DataMigrator — SSCursor streaming, batch COPY/INSERT, retry
    validate.py        # Validator — row counts + MD5 checksum comparison
    report.py          # MigrationReport + TableResult — thread-safe, Rich + JSON output
    wizard.py          # Interactive wizard (follows backup.py _wizard_* pattern)
```

New test files:
```
tests/test_migrate_types.py
tests/test_migrate_transform.py
tests/test_migrate_schema.py
tests/test_migrate_validate.py
tests/test_migrate_report.py
tests/test_migrate_runner.py
```

New doc:
```
docs/MIGRATION_GUIDE.md
```

---

## Critical Files to Modify

| File | Change |
|---|---|
| `fuelrod_backup/cli.py` | Add `migrate` command (typer) |
| `fuelrod_backup/adapters/mariadb.py` | Add `_query_rows()` returning `list[dict]` |
| `tests/conftest.py` | Add `MIGRATE_*` keys to `clean_env` fixture |

All other work is new files inside `fuelrod_backup/migrate/` and `tests/`.

---

## Type Mapping — `migrate/types.py`

`TypeMapper.map(data_type, column_type, is_unsigned, extra, ...) -> tuple[str, list[str]]`
Returns `(pg_type, warnings)`.

| MariaDB | PostgreSQL | Notes |
|---|---|---|
| `TINYINT(1)` | `BOOLEAN` | canonical bool pattern |
| `TINYINT` | `SMALLINT` | |
| `TINYINT UNSIGNED` | `SMALLINT` | 0-255 fits |
| `SMALLINT` | `SMALLINT` | |
| `SMALLINT UNSIGNED` | `INTEGER` | |
| `MEDIUMINT` / `MEDIUMINT UNSIGNED` | `INTEGER` | |
| `INT` / `INTEGER` | `INTEGER` | |
| `INT UNSIGNED` | `BIGINT` | 0-4294967295 |
| `BIGINT` | `BIGINT` | |
| `BIGINT UNSIGNED` | `NUMERIC(20)` | exceeds PG BIGINT max |
| `FLOAT` | `REAL` | |
| `DOUBLE` | `DOUBLE PRECISION` | |
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | `NUMERIC(p,s)` | copy precision |
| `BIT(n)` | `BIT(n)` | |
| `CHAR(n)` | `CHAR(n)` | |
| `VARCHAR(n)` | `VARCHAR(n)` | |
| `TINYTEXT` / `TEXT` / `MEDIUMTEXT` / `LONGTEXT` | `TEXT` | |
| `TINYBLOB` / `BLOB` / `MEDIUMBLOB` / `LONGBLOB` | `BYTEA` | |
| `BINARY(n)` / `VARBINARY(n)` | `BYTEA` | |
| `DATE` | `DATE` | |
| `TIME` | `TIME WITHOUT TIME ZONE` | |
| `DATETIME` | `TIMESTAMP WITHOUT TIME ZONE` | no tz in MySQL DATETIME |
| `TIMESTAMP` | `TIMESTAMP WITH TIME ZONE` | MySQL TIMESTAMP stores UTC |
| `YEAR` | `SMALLINT` | |
| `ENUM(...)` | `TEXT` + `CHECK (col IN (...))` | see ENUM edge case |
| `SET(...)` | `TEXT` + `CHECK` | or `TEXT[]` with `--split-set-to-array` |
| `JSON` | `JSONB` | validated during data migration |
| `GEOMETRY` / spatial | `TEXT` (warn) | requires PostGIS for full support |
| `AUTO_INCREMENT` (extra) | `GENERATED ALWAYS AS IDENTITY` | sequence reset after data load |
| `UNSIGNED` modifier | type promotion (see above) | no UNSIGNED in PG |
| `ZEROFILL` | stripped + warn | display-only; data unaffected |

---

## DDL Transformation Rules — `migrate/transform.py`

`SqlTransformer.transform_create_table(raw_ddl) -> TransformResult`

```python
@dataclass
class TransformResult:
    ddl: str             # clean PG CREATE TABLE
    post_ddl: list[str]  # ALTER SEQUENCE, CREATE INDEX, COMMENT ON …
    warnings: list[str]  # human-readable notes
```

Rules applied in order:

| Rule | Input | Output | Method |
|---|---|---|---|
| Backtick → double-quote | `` `name` `` | `"name"` | `_rewrite_backticks` |
| ENGINE clause | `ENGINE=InnoDB` | removed | `_strip_engine` |
| Charset / collate clause | `CHARSET=utf8mb4 COLLATE=…` | removed | `_strip_charset` |
| AUTO_INCREMENT column | `INT AUTO_INCREMENT` | `INTEGER GENERATED ALWAYS AS IDENTITY` | `_rewrite_autoincrement_col` |
| Table AUTO_INCREMENT start | `AUTO_INCREMENT=1001` | `ALTER SEQUENCE … RESTART WITH 1001` (post_ddl) | `_extract_autoincrement_start` |
| ENUM | `ENUM('a','b')` | `TEXT` + `CHECK (col IN ('a','b'))` (post_ddl) | `_rewrite_enum` |
| SET | `SET('x','y')` | `TEXT` with CHECK | `_rewrite_set` |
| BIT default | `DEFAULT b'0'` | `DEFAULT '0'::bit` | `_rewrite_bit_default` |
| BOOL default | `DEFAULT 1` on TINYINT(1) | `DEFAULT TRUE` | `_rewrite_bool_default` |
| ON UPDATE CURRENT_TIMESTAMP | present | stripped + warn | `_strip_on_update` |
| UNSIGNED check | UNSIGNED modifier | `CHECK (col >= 0)` if flag set | `_generate_unsigned_check` |
| KEY / INDEX | `KEY idx (col)` | `CREATE INDEX IF NOT EXISTS` (post_ddl) | `_extract_indexes` |
| FULLTEXT INDEX | `FULLTEXT KEY` | commented-out GIN suggestion (post_ddl) + warn | `_extract_indexes` |
| UNIQUE KEY | `UNIQUE KEY name (col)` | `UNIQUE (col)` inline | `_rewrite_unique` |
| FK backtick rewrite | `` REFERENCES `t` (`c`) `` | `REFERENCES "t" ("c")` | `_rewrite_fk` |
| DEFERRABLE FKs | FK constraints | + `DEFERRABLE INITIALLY DEFERRED` | `_rewrite_fk` |
| Column COMMENT | `COMMENT 'text'` | `COMMENT ON COLUMN` (post_ddl) | `_extract_col_comments` |
| Table COMMENT | `COMMENT='text'` | `COMMENT ON TABLE` (post_ddl) | `_extract_table_comment` |
| Column charset | `CHARACTER SET utf8mb4 COLLATE …` | stripped | `_strip_col_charset` |

---

## Schema Extraction — `migrate/schema.py`

`SchemaExtractor` uses `MariaDbAdapter._connect(dbname)` and a new `_query_rows(sql, params, dbname) -> list[dict]` helper added to `mariadb.py`.

Key information_schema queries:

```sql
-- Tables
SELECT TABLE_NAME, TABLE_COMMENT, AUTO_INCREMENT
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE';

-- Columns
SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE,
       DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
       DATETIME_PRECISION, COLUMN_TYPE, COLUMN_KEY, EXTRA, COLUMN_COMMENT,
       GENERATION_EXPRESSION
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
ORDER BY ORDINAL_POSITION;

-- Indexes
SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, INDEX_TYPE
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
ORDER BY INDEX_NAME, SEQ_IN_INDEX;

-- Foreign keys
SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME,
       kcu.REFERENCED_COLUMN_NAME, rc.UPDATE_RULE, rc.DELETE_RULE
FROM information_schema.KEY_COLUMN_USAGE kcu
JOIN information_schema.REFERENTIAL_CONSTRAINTS rc USING (CONSTRAINT_NAME, ...)
WHERE kcu.TABLE_SCHEMA = %s AND kcu.TABLE_NAME = %s;
```

Results collected in dataclasses:

```python
@dataclass
class DatabaseSchema:
    name: str
    tables: list[TableDef]
    views: list[ViewDef]
    triggers: list[TriggerDef]
    routines: list[RoutineDef]
    charset: str        # from SCHEMATA
    collation: str
```

---

## Schema Generation — `migrate/schema.py`

`SchemaGenerator.generate(schema, target_schema) -> GeneratedDDL`

```python
@dataclass
class GeneratedDDL:
    pre_data: list[str]     # CREATE TABLE, CREATE SEQUENCE, CREATE TYPE
    post_data: list[str]    # ALTER TABLE ADD CONSTRAINT FK, CREATE INDEX, COMMENT ON
    warnings: list[str]
    unsupported: list[str]  # triggers, procs — extracted to sidecar files
```

Key generation rules:
- All identifiers quoted with `"` — no reserved-word collisions
- FKs always `DEFERRABLE INITIALLY DEFERRED` (allows any insertion order)
- Identity column insert mode: `INSERT INTO … OVERRIDING SYSTEM VALUE VALUES (…)`
- After data migration: `SELECT setval(pg_get_serial_sequence('"tbl"','id'), MAX(id)) FROM "tbl";`
- ENUM default: `TEXT` + `CHECK` (optional `CREATE TYPE` via `--enum-as-type` flag)
- Views, triggers, routines: written to sidecar `.sql` files in commented form

---

## Data Migration — `migrate/data.py`

`DataMigrator.migrate_table(table, src_conn, dst_conn, column_types, pk_cols, batch_size, ...) -> TableResult`

Strategy:
1. Source: `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ` → consistent snapshot
2. Source cursor: `pymysql.cursors.SSCursor` (server-side, avoids full-table memory load)
3. Target: one `BEGIN` / `COMMIT` per batch (partial progress survives mid-table failure)
4. Method: `psycopg.copy.Copy` binary COPY when all columns have direct type mappings;
   fall back to parameterised `executemany` when Python-side coercion is needed (BLOB, ENUM, JSON)
5. Retry: up to 3 times with exponential backoff (2^attempt seconds) on transient errors
6. Skipped rows logged to `<db>_skipped_rows.jsonl`

Parallelism:
- `ThreadPoolExecutor(max_workers=parallel)` — one connection pair per worker
- Tables with > 1,000,000 rows bypass the pool (run sequentially, avoid memory pressure)
- `MigrationReport` uses `threading.Lock` for thread-safe recording

---

## Validation — `migrate/validate.py`

`Validator.validate_table(table, src_conn, dst_conn, pk_cols, column_names) -> ValidationResult`

Row count:
```sql
-- MariaDB
SELECT COUNT(*) FROM `<table>`
-- PostgreSQL
SELECT COUNT(*) FROM "public"."<table>"
```

MD5 checksum (only when `--validate-checksums` and no BLOB/FLOAT columns):
```sql
-- MariaDB
SELECT MD5(GROUP_CONCAT(MD5(CONCAT_WS('|', col1, col2, ...)) ORDER BY pk)) FROM `<table>`
-- PostgreSQL
SELECT MD5(STRING_AGG(MD5(CONCAT_WS('|', col1::text, col2::text, ...)), '' ORDER BY pk)) FROM "public"."<table>"
```

```python
@dataclass
class ValidationResult:
    table: str
    rows_source: int
    rows_dest: int
    count_match: bool
    checksum_source: str | None
    checksum_dest: str | None
    checksum_match: bool | None  # None = not run
    errors: list[str]
```

---

## Error Handling and Logging — `migrate/report.py`

```python
@dataclass
class TableResult:
    table: str
    status: Literal["ok", "failed", "skipped", "partial"]
    rows_source: int = 0
    rows_dest: int = 0
    rows_skipped: int = 0
    count_match: bool = False
    checksum_match: bool | None = None
    duration_s: float = 0.0
    warnings: list[str] = field(default_factory=list)
    error: str | None = None

class MigrationReport:
    def record(self, result: TableResult) -> None: ...    # thread-safe (Lock)
    def print_summary(self, console: Console) -> None: ...  # Rich table
    def write_json(self, path: Path) -> None: ...
```

- Each table migrates inside its own `try/except`; failure recorded but does not stop other tables
- `--fail-fast`: stops on first `status == "failed"`, rolls back any open transaction
- Rich `Progress` bar per table: spinner + row counter + percentage
- Exit code 1 if any table `status == "failed"`

---

## CLI Interface

Added to `fuelrod_backup/cli.py`:

```python
@app.command("migrate")
def migrate(
    source_db: str | None = ...,          # --source-db / -s
    target_db: str | None = ...,          # --target-db / -t
    target_schema: str = "public",        # --target-schema
    batch_size: int = 1000,               # --batch-size / -b
    parallel: int = 4,                    # --parallel / -p
    dry_run: bool = False,                # --dry-run
    no_interactive: bool = False,         # --no-interactive / -n
    validate: bool = True,                # --validate / --no-validate
    validate_checksums: bool = False,     # --validate-checksums
    fail_fast: bool = False,              # --fail-fast
    unsigned_checks: bool = False,        # --unsigned-checks / --no-unsigned-checks
    skip_tables: list[str] = [],          # --skip-table (repeatable)
    only_tables: list[str] = [],          # --only-table (repeatable)
    report_file: Path | None = None,      # --report / -r
    config_file: Path | None = ...,       # --config / -c
) -> None:
```

Loads `MY_*` config as `src_cfg` and `PG_*` config as `dst_cfg` via `load_all_configs()`.
Errors immediately if either is missing. Delegates to `run_migrate()` in `migrate/__init__.py`.

---

## Config Format

No new config keys required — the existing multi-engine format provides everything:

```ini
# .backup
MY_USERNAME=root
MY_PASSWORD=secret
MY_HOST=127.0.0.1
MY_PORT=3306
MY_SERVICE=mariadb
MY_USE_DOCKER=true

PG_USERNAME=postgres
PG_PASSWORD=secret
PG_HOST=127.0.0.1
PG_PORT=5432
PG_USE_DOCKER=false
```

Optional migration-specific tunables (read directly from env/file in `run_migrate()`; not in Config dataclass):

```ini
MIGRATE_BATCH_SIZE=1000
MIGRATE_PARALLEL=4
MIGRATE_DRY_RUN=false
MIGRATE_VALIDATE=true
MIGRATE_TARGET_SCHEMA=public
MIGRATE_UNSIGNED_CHECKS=false
```

---

## Edge Case Summary

| Edge Case | Handling |
|---|---|
| `AUTO_INCREMENT` | `GENERATED ALWAYS AS IDENTITY`; `OVERRIDING SYSTEM VALUE` during insert; sequence reset after data |
| `ENUM` | `TEXT` + `CHECK` (default); `CREATE TYPE` with `--enum-as-type` |
| `SET` | `TEXT` with CHECK; `TEXT[]` with `--split-set-to-array` |
| `JSON` | Mapped to `JSONB`; validated via `json.loads()` per row; invalid rows skipped + logged |
| `BLOB/VARBINARY` | Mapped to `BYTEA`; pymysql returns `bytes`; passed as-is to psycopg3 |
| `TINYINT(1)` | `BOOLEAN`; pymysql returns 0/1; coerced to `True`/`False` |
| `UNSIGNED` | Type promoted (see table); optional `CHECK (col >= 0)` |
| `ZEROFILL` | Stripped + warn |
| `ON UPDATE CURRENT_TIMESTAMP` | Stripped + warn + suggested PL/pgSQL trigger body in report |
| `FULLTEXT INDEX` | Commented-out GIN suggestion; warn |
| Views | Commented-out DDL in `<db>_views_mariadb.sql`; MySQL functions flagged |
| Triggers | Extracted to `<db>_triggers_mariadb.sql`; not applied |
| Stored procs | Extracted to `<db>_routines_mariadb.sql`; not applied |
| Encoding (latin1→UTF8) | Detect charset; `encode('latin1').decode('utf8')`; log failures |
| FK ordering | All FKs `DEFERRABLE INITIALLY DEFERRED` |
| Reserved word collision | All identifiers double-quoted |
| Large tables (> 1M rows) | Bypass thread pool; run in main thread |

---

## Performance Considerations

- **Server-side cursor** (`SSCursor`) on MariaDB — never loads a full table into Python memory
- **Binary COPY** via `psycopg.copy.Copy` where possible — fastest path into PostgreSQL
- **Batch size**: default 1000 rows; tune with `--batch-size` or `MIGRATE_BATCH_SIZE`
- **Parallelism**: default 4 workers; each worker owns its own connection pair
- **Large tables**: > 1,000,000 rows run sequentially in the main thread
- **FK deferral**: avoids per-row constraint checks during bulk load
- **Sequence reset**: done after all data is loaded, not during
- **PostgreSQL tuning**: before migration, `SET work_mem = '256MB'` and `SET maintenance_work_mem = '1GB'` on the target session (emitted as advisory notes in the wizard)

---

## Rollback and Recovery

- Source MariaDB is **never modified** — the tool is read-only on the source
- PostgreSQL target database can be dropped with `DROP DATABASE` and recreated cleanly
- `--dry-run` runs schema extraction, DDL generation, and prints the full plan without writing anything
- Each table has its own transaction scope — a failed table does not affect successfully committed tables
- Skipped rows are recorded in `<db>_skipped_rows.jsonl` for manual inspection and re-import

---

## Testing Plan

### Unit tests (no DB required)
- `test_migrate_types.py` — parametrized type mapping matrix (every row in the type table)
- `test_migrate_transform.py` — DDL rewriting rules with string fixtures
- `test_migrate_report.py` — thread-safe recording, JSON output, Rich summary

### Integration-style tests (monkeypatched DB)
- `test_migrate_schema.py` — `SchemaExtractor` with mocked pymysql cursor
- `test_migrate_validate.py` — `Validator` with mocked cursor results
- `test_migrate_runner.py` — `MigrateRunner` with both adapters monkeypatched:
  - dry-run creates no tables
  - data migration creates table, inserts rows, resets sequence
  - table failure does not stop other tables
  - `--fail-fast` stops on first error
  - checksum mismatch produces `partial` status

`conftest.py` additions: add `MIGRATE_*` keys to the `_CONFIG_ENV_KEYS` list in `clean_env`.

---

## Implementation Order

Build inward-out to keep each layer independently testable:

1. `migrate/types.py` — `TypeMapper`
2. `migrate/transform.py` — `SqlTransformer` + `TransformResult`
3. `migrate/report.py` — `MigrationReport` + `TableResult`
4. `migrate/schema.py` — `SchemaExtractor` + `SchemaGenerator` + `DatabaseSchema`
5. `mariadb.py` — add `_query_rows()` helper
6. `migrate/validate.py` — `Validator` + `ValidationResult`
7. `migrate/data.py` — `DataMigrator`
8. `migrate/runner.py` — `MigrateRunner`
9. `migrate/wizard.py` — interactive wizard
10. `migrate/__init__.py` — `run_migrate()` public entry point
11. `cli.py` — `migrate` command
12. All test files
13. `docs/MIGRATION_GUIDE.md`

---

## Verification

```bash
# 1. Unit tests pass
poetry run pytest tests/test_migrate_types.py tests/test_migrate_transform.py tests/test_migrate_report.py -v

# 2. Integration-style tests pass (monkeypatched)
poetry run pytest tests/test_migrate_schema.py tests/test_migrate_runner.py -v

# 3. Full test suite still passes (no regressions)
poetry run pytest

# 4. Dry-run against real MariaDB + PostgreSQL (read-only)
poetry run fuelrod-backup migrate --source-db mydb --dry-run

# 5. Live migration of a small test database
poetry run fuelrod-backup migrate --source-db mydb --target-db mydb_pg --validate --report migration.json

# 6. Inspect report
cat migration.json | python -m json.tool

# 7. Verify row counts manually
poetry run fuelrod-backup test --db-type mariadb
poetry run fuelrod-backup test --db-type postgres
```

---

## docs/MIGRATION_GUIDE.md — Sections to Write

1. Overview — what the tool does / does not do
2. Prerequisites — MY_* + PG_* config, user privileges
3. Quick start — one-command example
4. Configuration reference — MIGRATE_* keys table
5. Type mapping reference — full table
6. DDL transformation rules — before/after examples
7. Edge cases — ENUM, AUTO_INCREMENT, JSON, BLOB, charset, views/triggers/procs
8. Validation — row count + checksum, report format
9. Rollback — how to undo, dry-run workflow
10. Performance tuning — batch size, parallelism, PG server settings
11. Troubleshooting — common errors with solutions
12. Migration report JSON schema
13. Limitations — spatial, user-defined types, events, full-text

---

## Risk Summary (Top 5)

| Risk | Mitigation |
|---|---|
| Data loss from skipped rows | `rows_skipped` tracked; `partial` status; `<db>_skipped_rows.jsonl` sidecar |
| Stored procs / triggers incompatible | Extracted to sidecar SQL files; report marks each `SKIPPED` |
| Encoding corruption (latin1 → UTF-8) | Charset detection before migration; encode/decode with fallback; log failures |
| Semantic drift: DATETIME ≠ TIMESTAMP | `DATETIME` → `TIMESTAMP WITHOUT TIME ZONE`; documented; user must verify app tz assumptions |
| Source DB written to during migration | REPEATABLE READ isolation per table; recommend read-only source; documented |
