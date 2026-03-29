# Architecture and Workflows

## High-level Modules

- `fuelrod_backup/cli.py`: Typer commands (`backup`, `restore`, `test`, `init`)
- `fuelrod_backup/config.py`: config parsing and merge logic
- `fuelrod_backup/adapters/*`: engine implementations for postgres, mariadb, mssql
- `fuelrod_backup/backup.py`: backup wizard + execution + retention cleanup
- `fuelrod_backup/restore.py`: restore wizard + execution flow
- `fuelrod_backup/runner.py`: PostgreSQL-specific native + subprocess operations
- `fuelrod_backup/prompt.py`: questionary wrapper with safe Ctrl+C behavior

## Backup Flow

1. Load config and apply CLI overrides.
2. Resolve adapter from `DB_TYPE`.
3. Validate connection.
4. Select databases (wizard) or use provided/all databases.
5. Dump each database to engine-specific format.
6. Optionally compress dump (`.gz` except MSSQL `.bak`).
7. Remove old backups based on `KEEP_DAYS`.

## Restore Flow

1. Validate config and connection.
2. Select database directory and backup file.
3. Run engine-specific restore path:
   - PostgreSQL: TOC analysis, optional schema/table filtering, role handling, `pg_restore`
   - MariaDB: restore SQL input via client
   - MSSQL: restore `.bak` with T-SQL
4. Print restore summary and post-restore stats (PostgreSQL).

## Adapter Capabilities

Each adapter exposes capability flags used by restore logic:

- `supports_schemas`
- `supports_roles`
- `supports_toc`
- `dump_extension`
