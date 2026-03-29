# fuelrod-backup

Interactive backup and restore CLI for PostgreSQL, MariaDB/MySQL, and Microsoft SQL Server.

## Features

- Supports `postgres`, `mariadb`, and `mssql` engines
- Interactive backup and restore wizards
- Non-interactive backup mode for automation
- Docker and direct-host connection modes
- Config bootstrap wizard (`init`) and connection check (`test`)
- Backup retention cleanup (`KEEP_DAYS`)

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
- [Code Review Notes](docs/CODE_REVIEW.md)
- [Feature Plan](docs/FEATURE_PLAN.md)
- [Feature Checklist](docs/FEATURE_CHECKLIST.md)
- Example config: [`.backup.example`](.backup.example)

## CLI Commands

```text
fuelrod-backup backup [OPTIONS]
fuelrod-backup restore [OPTIONS]
fuelrod-backup test [OPTIONS]
fuelrod-backup init [OPTIONS]
```

### `backup` options

- `--no-interactive, -n` skip prompts and back up all databases
- `--compress / --no-compress` override compression
- `--keep-days, -k` override retention days (`0` keeps forever)
- `--db, -d` repeatable database selector
- `--docker / --no-docker` override `USE_DOCKER`
- `--db-type, -t` one of `postgres | mariadb | mssql`
- `--config, -c` explicit config file path

### `restore` and `test` options

- `--docker / --no-docker`
- `--db-type, -t`
- `--config, -c`

### `init` options

- `--output, -o` output path for generated config (default `.backup`)

## Config

Config is loaded in this order:

1. defaults in code
2. config file (`--config` if passed, otherwise auto-discovered)
3. environment variables (highest priority)

### Auto-discovery order

For each directory below, files are checked in this order:

1. `.backup`
2. `.env`
3. `.env-backup`

Directories searched:

1. current working directory
2. project directory
3. parent repo directory

### Main keys

- `DB_TYPE=postgres|mariadb|mssql`
- `DB_USERNAME`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT` 
- `USE_DOCKER=true|false`
- `SERVICE=<container-name>`
- `BASE_DIR=<backup-root>`
- `COMPRESS_FILE=true|false`
- `KEEP_DAYS=<int>`
- `CONNECTION_TIMEOUT=<seconds>`

PostgreSQL-specific:

- `PG_DUMP_CMD`
- `PG_RESTORE_CMD`

MariaDB-specific:

- `MYSQL_DUMP_CMD`
- `MYSQL_CMD`

MSSQL-specific:

- `MSSQL_BACKUP_DIR`

## Notes

- Effective backup output directory is `<BASE_DIR>/<DB_TYPE>/...`.
- PostgreSQL backups use custom format `.dump` (optional `.gz`).
- MariaDB backups use `.sql` (optional `.gz` or `.zip` input supported on restore).
- MSSQL backups use `.bak`.

## License

GNU General Public License v3.0 or later. See [LICENSE](LICENSE).


## Breaking Change

Connection variables are now engine-agnostic only:

- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`

Legacy connection keys (`PG_USERNAME`, `PG_PASSWORD`, `PG_HOST`, `PG_PORT`) are no longer loaded.
