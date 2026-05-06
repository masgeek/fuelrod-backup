# fuelrod-backup

Interactive backup and restore CLI for PostgreSQL, MariaDB/MySQL, and Microsoft SQL Server.

## Features

- Supports `postgres`, `mariadb`, and `mssql` engines
- Interactive backup and restore wizards with progress bars
- Non-interactive backup mode for automation
- Docker and direct-host connection modes
- Config bootstrap wizard (`init`) and connection check (`test`)
- Backup retention cleanup (`KEEP_DAYS`)
- Interactive database and schema drop (`drop`)
- n8n Docker volume snapshots with no container downtime
- Google Drive sync via rclone (`gdrive-sync`)
- MariaDB → PostgreSQL migration with schema transformation, data migration, and post-migration validation (`migrate`)

## Installation

```bash
pip install fuelrod-backup
```

Or with `pipx`:

```bash
pipx install fuelrod-backup
```

## Quick Start

1. Initialize config:

```bash
fuelrod-backup init
```

2. Validate connection:

```bash
fuelrod-backup test
```

3. Run backup:

```bash
fuelrod-backup backup
```

4. Run restore:

```bash
fuelrod-backup restore
```

## Documentation

- [Configuration Reference](docs/CONFIGURATION.md)
- [Architecture and Workflows](docs/ARCHITECTURE.md)
- [Migration Guide](docs/MIGRATION_GUIDE.md)
- [Feature Plan](docs/FEATURE_PLAN.md)
- [Feature Checklist](docs/FEATURE_CHECKLIST.md)
- Example config: [`.backup.example`](.backup.example)

## CLI Commands

```text
fuelrod-backup init       [OPTIONS]
fuelrod-backup test       [OPTIONS]
fuelrod-backup backup     [OPTIONS]
fuelrod-backup restore    [OPTIONS]
fuelrod-backup drop       [OPTIONS]
fuelrod-backup migrate    [OPTIONS]
fuelrod-backup n8n-backup  [OPTIONS]
fuelrod-backup n8n-restore [OPTIONS]
fuelrod-backup gdrive-sync [OPTIONS]
```

The short alias `fr-bkp` is equivalent to `fuelrod-backup` for all commands.

### `init`

Wizard to generate a `.backup` config file.

- `--output, -o PATH` — output path (default: `.backup`)

### `test`

Validate connection and print the resolved config.

- `--docker / --no-docker`
- `--db-type, -t` — `postgres | mariadb | mssql`
- `--config, -c PATH`

### `backup`

Interactive or non-interactive backup with optional retention cleanup.

- `--no-interactive, -n` — skip prompts and back up all databases
- `--all-engines, -a` — back up all engines in parallel (multi-engine config)
- `--compress / --no-compress` — override `COMPRESS_FILE`
- `--keep-days, -k INT` — override `KEEP_DAYS` (`0` keeps forever)
- `--db, -d TEXT` — repeatable; restrict to specific database(s)
- `--docker / --no-docker`
- `--db-type, -t`
- `--config, -c PATH`

### `restore`

Interactive restore with engine-specific options. PostgreSQL includes TOC analysis for schema, table, and role filtering.

- `--all-engines, -a` — restore all engines sequentially (multi-engine config)
- `--docker / --no-docker`
- `--db-type, -t`
- `--config, -c PATH`

### `drop`

Interactive database or schema drop. Terminates active connections before dropping and requires confirmation.

- `--docker / --no-docker`
- `--db-type, -t`
- `--config, -c PATH`

### `migrate`

Migrate a MariaDB database to PostgreSQL. Extracts schema from `information_schema`, transforms DDL, migrates data in batches, and validates row counts.

- `--source-db, -s TEXT` — source MariaDB database
- `--target-db, -t TEXT` — target PostgreSQL database
- `--target-schema TEXT` — PostgreSQL schema (default: `public`)
- `--batch-size, -b INT` — rows per INSERT batch (default: `1000`)
- `--parallel, -p INT` — table-level parallelism (default: `4`)
- `--dry-run` — plan only, no writes
- `--no-interactive, -n` — skip wizard
- `--validate / --no-validate` — row-count reconciliation after migration (default: on)
- `--validate-checksums` — also compare MD5 checksums (slower)
- `--fail-fast` — stop on first table failure
- `--unsigned-checks / --no-unsigned-checks` — generate `CHECK (col >= 0)` for `UNSIGNED` columns
- `--enum-as-type / --enum-as-check` — map `ENUM` to `CREATE TYPE` instead of `TEXT + CHECK`
- `--skip-table TEXT` — repeatable; exclude table(s)
- `--only-table TEXT` — repeatable; migrate only these table(s)
- `--report, -r PATH` — write JSON migration report to file
- `--config, -c PATH`

### `n8n-backup`

Hot snapshot of n8n Docker volumes as `.tar.gz` archives. No container downtime required.

- `--no-interactive, -n` — back up all services
- `--service, -s TEXT` — repeatable; restrict to specific service(s)
- `--config, -c PATH`

### `n8n-restore`

Restore an n8n volume from a `.tar.gz` archive.

- `--service, -s TEXT` — service name
- `--file, -f PATH` — path to backup archive
- `--dry-run` — show plan without making changes
- `--verbose, -v` — print detailed step logs
- `--config, -c PATH`

### `gdrive-sync`

Upload backup files to Google Drive via rclone, with optional age-based remote pruning.

- `--dry-run, -d` — show plan without uploading
- `--gdrive, -g TEXT` — Google Drive remote folder name
- `--days, -n INT` — prune remote files older than N days
- `--include, -i GLOB` — repeatable; glob pattern to include
- `--keep-local` — skip local file deletion after upload
- `--config, -c PATH`

## Config

Config is loaded in this order:

1. Internal defaults
2. Config file (`--config` if passed, otherwise auto-discovered)
3. Environment variables (highest priority)

### Auto-discovery order

For each directory below, files are checked in this order: `.backup`, `.env`, `.env-backup`

Directories searched:

1. current working directory
2. project directory
3. parent repo directory

### Required keys

```
DB_TYPE=postgres|mariadb|mssql
DB_USERNAME=
DB_PASSWORD=
DB_HOST=
DB_PORT=
```

### Optional keys

| Key | Description |
|-----|-------------|
| `USE_DOCKER` | `true`/`false` — connect via Docker exec |
| `SERVICE` | container name (Docker mode) |
| `BASE_DIR` | backup root directory |
| `COMPRESS_FILE` | `true`/`false` — gzip output |
| `KEEP_DAYS` | retention days (`0` = keep forever) |
| `CONNECTION_TIMEOUT` | connection timeout in seconds |

### Engine-specific keys

PostgreSQL:

| Key | Default | Description |
|-----|---------|-------------|
| `PG_DUMP_CMD` | `pg_dump` | pg_dump executable |
| `PG_RESTORE_CMD` | `pg_restore` | pg_restore executable |
| `PG_CMD` | `psql` | psql executable |

MariaDB/MySQL:

| Key | Default | Description |
|-----|---------|-------------|
| `MYSQL_DUMP_CMD` | `mariadb-dump` | dump executable |
| `MYSQL_CMD` | `mysql` | client executable |

MSSQL:

| Key | Default | Description |
|-----|---------|-------------|
| `MSSQL_BACKUP_DIR` | — | backup directory path inside container |

## Output layout

```
<BASE_DIR>/
  postgres/<database>/<YYYYMMDD-HHMMSS>.dump[.gz]
  mariadb/<database>/<YYYYMMDD-HHMMSS>.sql[.gz]
  mssql/<database>/<YYYYMMDD-HHMMSS>.bak
```

## Notes

- PostgreSQL backups use custom format (`.dump`). Compression wraps the dump in `.gz`.
- MariaDB backups use `.sql`. Restore accepts `.sql`, `.sql.gz`, and `.zip`.
- MSSQL backups use native `.bak` format (no gzip).
- Config files generated by `init` are written with `0o600` permissions.

## License

GNU General Public License v3.0 or later. See [LICENSE](LICENSE).

## Breaking Change

Connection variables are now engine-agnostic only:

- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`

Legacy connection keys (`PG_USERNAME`, `PG_PASSWORD`, `PG_HOST`, `PG_PORT`) are no longer loaded.
