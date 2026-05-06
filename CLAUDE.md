# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`fuelrod-backup` is an interactive CLI for backing up and restoring PostgreSQL, MariaDB/MySQL, and MSSQL databases, with support for Docker containers, n8n volume snapshots, Google Drive sync via rclone, and MariaDB → PostgreSQL migration.

## Development Commands

```bash
# Install dependencies
poetry install

# Run the CLI
poetry run fuelrod-backup <command>
poetry run fr-bkp <command>         # short alias

# Run tests
poetry run pytest

# Lint
poetry run ruff check .

# Build distribution
poetry build
```

**Python requirement:** `>=3.13`

## Architecture

### Adapter Pattern

All database engines implement `adapters/base.py::DbAdapter`. Use `adapters/__init__.py::get_adapter(config)` to instantiate the correct adapter based on `config.db_type`.

Each adapter declares capability flags:
- `supports_schemas`, `supports_roles`, `supports_toc`, `supports_schema_drop` — controls what restore/drop options are offered
- `dump_extension` — engine-specific file extension

Adapters: `postgres.py`, `mariadb.py`, `mssql.py`. When adding a new engine, subclass `DbAdapter` and register in the factory.

### Config Loading (`config.py`)

Config is loaded in layers: **defaults → file → environment variables**. File auto-discovery searches for `.backup`, `.env`, `.env-backup` walking up from cwd to repo root. Engine-agnostic keys are required (`DB_USERNAME`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_TYPE`). Engine-specific keys (e.g. `PG_DUMP_CMD`) are optional overrides.

Multi-engine mode uses `PG_*`, `MY_*`, `MS_*` key prefixes and `load_all_configs()`.

### CLI Structure (`cli.py`)

Main commands:
- `init` — wizard to generate `.backup` config
- `test` — validate connection and print resolved config
- `backup` — interactive or non-interactive backup with retention cleanup and progress bars
- `restore` — interactive restore with PostgreSQL TOC/schema/role filtering
- `drop` — interactive database or schema drop with connection termination and confirmation
- `migrate` — MariaDB → PostgreSQL migration (schema, data, validation, report)
- `n8n-backup` / `n8n-restore` — hot Docker volume snapshots
- `gdrive-sync` — upload backups to Google Drive via rclone

### PostgreSQL-Specific Details

`runner.py::PgRunner` handles all native PostgreSQL operations using psycopg3 (no psql subprocess). Subprocesses are only used for `pg_dump`, `pg_restore`, and `docker cp`. Subprocess calls must never inherit the parent environment — use explicit `env={}` to prevent credential leakage.

### Subprocess Security Rule

All subprocess calls that invoke database tools must pass an explicit, minimal `env` dict. Do not use `subprocess.run(..., env=os.environ)` or omit `env=` when executing `pg_dump`, `mysqldump`, `pg_restore`, or similar tools.

### Migration Subsystem (`migrate/`)

The `migrate/` package implements MariaDB → PostgreSQL migration. Key modules:

| Module | Role |
|--------|------|
| `types.py` | `TypeMapper` — MariaDB → PostgreSQL type mapping (20+ types) |
| `transform.py` | `SqlTransformer` — DDL rewrite rules (15+): backticks, ENGINE, CHARSET, AUTOINCREMENT, ENUM, SET, indexes, FK DEFERRABLE, ZEROFILL, comments |
| `schema.py` | `SchemaExtractor` (information_schema queries) + `SchemaGenerator` (DDL builder) |
| `data.py` | `DataMigrator` — SSCursor streaming, batched INSERT, 3-retry backoff |
| `validate.py` | `Validator` — row-count + optional MD5 checksum reconciliation |
| `report.py` | `MigrationReport` + `TableResult` — thread-safe recording, Rich + JSON output |
| `runner.py` | `MigrateRunner` — pipeline orchestrator, parallelism, large-table handling |
| `wizard.py` | Interactive selection wizard |
| `__init__.py` | `run_migrate()` — public entry point |

S608 (SQL injection lint warning) is suppressed with `# noqa: S608` at each specific call site in this package. These queries use schema/table names sourced from internal `information_schema` metadata, not from user input.

### n8n and Google Drive

- `n8n_backup.py` / `n8n_restore.py` — Docker volume tar.gz snapshots, no container downtime required
- `gdrive_sync.py` — wraps `rclone-python` library; uses glob include patterns and age-based pruning

## Key Dependencies

| Package | Purpose |
|---|---|
| `typer` | CLI framework |
| `questionary` | Interactive prompts (wrapped in `prompt.py` for safe Ctrl+C) |
| `rich` | Terminal formatting and progress bars |
| `psycopg[binary]` | PostgreSQL native driver |
| `pymysql` | MariaDB/MySQL driver |
| `pymssql` | MSSQL driver |
| `rclone-python` | Google Drive sync |

## Configuration Reference

See `.backup.example` for all supported keys. Required keys:

```
DB_TYPE=postgres|mariadb|mssql
DB_USERNAME=
DB_PASSWORD=
DB_HOST=
DB_PORT=
```

Optional: `USE_DOCKER`, `SERVICE`, `BASE_DIR`, `COMPRESS_FILE`, `KEEP_DAYS`, `CONNECTION_TIMEOUT`

Engine-specific: `PG_DUMP_CMD`, `PG_RESTORE_CMD`, `PG_CMD`, `MYSQL_DUMP_CMD`, `MYSQL_CMD`, `MSSQL_BACKUP_DIR`

## Feature Roadmap

See `docs/FEATURE_PLAN.md` and `docs/FEATURE_CHECKLIST.md` for the prioritized roadmap. Shipped features (backup, restore, drop, migrate, n8n, gdrive-sync) are tracked there. Planned work includes non-interactive restore, structured logging, `doctor` command, and S3 storage backends.
