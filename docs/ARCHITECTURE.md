# Architecture and Workflows

## High-level Modules

| Module | Responsibility |
|--------|---------------|
| `cli.py` | Typer app — all command entry points |
| `config.py` | Config parsing, three-layer merge, auto-discovery |
| `adapters/` | Engine implementations (postgres, mariadb, mssql) |
| `backup.py` | Backup wizard, execution, retention cleanup |
| `restore.py` | Restore wizard, TOC parsing, engine-specific restore |
| `runner.py` | PostgreSQL native + subprocess operations (psycopg3) |
| `drop.py` | Interactive database/schema drop |
| `n8n_backup.py` | Hot Docker volume snapshots for n8n |
| `n8n_restore.py` | Volume archive restore for n8n |
| `gdrive_sync.py` | Google Drive upload via rclone |
| `migrate/` | MariaDB → PostgreSQL migration subsystem |
| `prompt.py` | questionary wrapper with safe Ctrl+C handling |

## Adapter Pattern

All engines implement `adapters/base.py::DbAdapter`. The factory `adapters/__init__.py::get_adapter(config)` returns the correct subclass based on `config.db_type`.

Each adapter declares capability flags that control which restore and drop options are offered:

| Flag | PG | MariaDB | MSSQL |
|------|----|---------|-------|
| `supports_schemas` | ✓ | | ✓ |
| `supports_roles` | ✓ | | |
| `supports_toc` | ✓ | | |
| `supports_schema_drop` | ✓ | | |
| `dump_extension` | `.dump` | `.sql` | `.bak` |

## Backup Flow

1. Load config; apply CLI overrides.
2. Resolve adapter from `DB_TYPE`.
3. Validate connection.
4. Select databases (wizard) or use provided/all databases.
5. Dump each database to the engine-specific format.
6. Optionally compress output (`.gz`; not applicable to MSSQL `.bak`).
7. Remove old backups based on `KEEP_DAYS`; delete empty directories.

Multi-engine mode (`--all-engines`) runs each engine in parallel via `ThreadPoolExecutor`.

## Restore Flow

1. Validate config and connection.
2. Select database directory and backup file from the backup tree.
3. Run engine-specific restore:
   - **PostgreSQL** — TOC analysis (`pg_restore -l`); optional schema/table/role filtering; `pg_restore`.
   - **MariaDB** — pipe `.sql` (or decompress `.sql.gz` / `.zip`) through the MySQL client.
   - **MSSQL** — T-SQL `RESTORE DATABASE` via pymssql; copies `.bak` into container with `docker cp` if needed.
4. Print restore summary and post-restore stats (PostgreSQL).

## Drop Flow

1. List databases (or schemas for PostgreSQL).
2. Prompt for selection and require a confirmation token.
3. Terminate active connections (`pg_terminate_backend` for PostgreSQL).
4. Execute `DROP DATABASE` / `DROP SCHEMA ... CASCADE`.

## Migration Flow (MariaDB → PostgreSQL)

```
MariaDB (information_schema)
        │
        ▼
  SchemaExtractor        ← reads tables, columns, indexes, FKs, views, triggers, routines
        │
        ▼
  SchemaGenerator        ← builds PostgreSQL DDL (identity cols, CHECK constraints, deferred FKs)
        │
        ▼
  SqlTransformer         ← rewrites raw CREATE TABLE DDL (15+ rules: backticks, ENGINE, CHARSET,
        │                   AUTOINCREMENT, ENUM, SET, indexes, FK DEFERRABLE, comments, ZEROFILL)
        ▼
  PgRunner._execute()    ← applies DDL to target PostgreSQL database
        │
        ▼
  DataMigrator           ← streams rows via SSCursor; batched INSERT; 3-retry exponential backoff
        │
        ▼
  Validator              ← row-count reconciliation; optional MD5 checksum comparison
        │
        ▼
  MigrationReport        ← thread-safe result recording; Rich terminal table + JSON file output
```

Key migration behaviours:

- `AUTO_INCREMENT` → `GENERATED ALWAYS AS IDENTITY`
- `TINYINT(1)` → `BOOLEAN`
- `ENUM` → `TEXT + CHECK` (or `CREATE TYPE` with `--enum-as-type`)
- `JSON` → `JSONB`
- `BLOB` / `VARBINARY` → `BYTEA`
- `BIT(1)` → `BOOLEAN`; `BIT(n>1)` → `BIT(n)`
- `ZEROFILL` stripped with warning
- Foreign keys appended with `DEFERRABLE INITIALLY DEFERRED`
- Tables with > 1 million rows bypass the thread pool and run sequentially
- Skipped rows written to `<db>_skipped_rows.jsonl`

## n8n Volume Backup/Restore Flow

**Backup** — for each n8n service, `docker exec` + `tar` creates a `.tar.gz` of the volume contents. The container keeps running throughout.

**Restore** — extracts the archive into the volume with `docker cp` + `tar`; `--dry-run` prints the plan without touching files.

## Google Drive Sync Flow

1. Resolve backup files matching glob include patterns.
2. Upload to the configured rclone remote folder.
3. Prune remote files older than `--days` (default: 2 days).
4. Optionally delete local files after successful upload.

## PostgreSQL Runner

`runner.py::PgRunner` handles all PostgreSQL operations using psycopg3 as the native driver. Subprocesses are used **only** for `pg_dump`, `pg_restore`, and `docker cp`. All subprocess calls pass an explicit minimal `env` dict — the parent environment is never inherited — to prevent credential leakage.

## Config Loading

Three-layer merge: **internal defaults → config file → environment variables**.

Auto-discovery walks up from the current directory searching for `.backup`, `.env`, `.env-backup`. Multi-engine mode loads per-engine configs via `PG_*`, `MY_*`, `MS_*` key prefixes using `load_all_configs()`.
