# Configuration Reference

This tool loads configuration from three layers (lowest to highest priority):

1. Internal defaults
2. Config file (`--config` path, or auto-discovered file)
3. Environment variables

## Auto-discovery

Directories searched in order:

1. current working directory
2. project directory
3. parent repo directory

File names checked in each directory:

1. `.backup`
2. `.env`
3. `.env-backup`

## Core Variables

- `DB_TYPE` (`postgres`, `mariadb`, `mssql`)
- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `USE_DOCKER` (`true`/`false`)
- `SERVICE` (container name in Docker mode)
- `BASE_DIR` (backup root)
- `COMPRESS_FILE` (`true`/`false`)
- `KEEP_DAYS` (`0` disables cleanup)
- `CONNECTION_TIMEOUT` (seconds)

## Engine-specific Variables

PostgreSQL:

- `PG_DUMP_CMD`
- `PG_RESTORE_CMD`

MariaDB/MySQL:

- `MYSQL_DUMP_CMD`
- `MYSQL_CMD`

MSSQL:

- `MSSQL_BACKUP_DIR` (path inside container when Docker mode is enabled)

## Output Layout

Backups are stored under:

`<BASE_DIR>/<DB_TYPE>/<database>/`


