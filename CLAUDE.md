# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`fuelrod-backup` is an interactive CLI for backing up and restoring PostgreSQL, MariaDB/MySQL, and MSSQL databases, with support for Docker containers, n8n volume snapshots, and Google Drive sync via rclone.

## Development Commands

```bash
# Install dependencies
poetry install

# Run the CLI
poetry run fuelrod-backup <command>
poetry run fr-bkp <command>         # short alias

# Run tests
poetry run pytest

# Build distribution
poetry build
```

**Python requirement:** `>=3.13`

## Architecture

### Adapter Pattern

All database engines implement `adapters/base.py::DbAdapter`. Use `adapters/__init__.py::get_adapter(config)` to instantiate the correct adapter based on `config.db_type`.

Each adapter declares capability flags:
- `supports_schemas`, `supports_roles`, `supports_toc` — controls what restore options are offered
- `dump_extension` — engine-specific file extension

Adapters: `postgres.py`, `mariadb.py`, `mssql.py`. When adding a new engine, subclass `DbAdapter` and register in the factory.

### Config Loading (`config.py`)

Config is loaded in layers: **defaults → file → environment variables**. File auto-discovery searches for `.backup`, `.env`, `.env-backup` walking up from cwd to repo root. Engine-agnostic keys are required (`DB_USERNAME`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_TYPE`). Engine-specific keys (e.g. `PG_DUMP_CMD`) are optional overrides.

### CLI Structure (`cli.py`)

Main commands:
- `init` — wizard to generate `.backup` config
- `test` — validate connection and print resolved config
- `backup` — interactive or non-interactive backup with retention cleanup
- `restore` — interactive restore with PostgreSQL TOC/schema/role filtering
- `n8n-backup` / `n8n-restore` — hot Docker volume snapshots
- `gdrive-sync` — upload backups to Google Drive via rclone

### PostgreSQL-Specific Details

`runner.py::PgRunner` handles all native PostgreSQL operations using psycopg3 (no psql subprocess). Subprocesses are only used for `pg_dump`, `pg_restore`, and `docker cp`. Subprocess calls must never inherit the parent environment — use explicit `env={}` to prevent credential leakage.

### Subprocess Security Rule

All subprocess calls that invoke database tools must pass an explicit, minimal `env` dict. Do not use `subprocess.run(..., env=os.environ)` or omit `env=` when executing `pg_dump`, `mysqldump`, `pg_restore`, or similar tools.

### n8n and Google Drive

- `n8n_backup.py` / `n8n_restore.py` — Docker volume tar.gz snapshots, no container downtime required
- `gdrive_sync.py` — wraps `rclone-python` library; uses glob include patterns and age-based pruning

## Key Dependencies

| Package | Purpose |
|---|---|
| `typer` | CLI framework |
| `questionary` | Interactive prompts (wrapped in `prompt.py` for safe Ctrl+C) |
| `rich` | Terminal formatting |
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

## Feature Roadmap

See `docs/FEATURE_PLAN.md` and `docs/FEATURE_CHECKLIST.md` for the prioritized roadmap (non-interactive restore, structured logging, pre-flight checks, etc.).
