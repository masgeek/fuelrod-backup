# fuelrod-backup

Interactive backup and restore CLI for PostgreSQL, MariaDB/MySQL, and Microsoft SQL Server.

## Features

- Supports `postgres`, `mariadb`, and `mssql` engines
- Interactive backup and restore wizards with progress bars
- Non-interactive backup and restore modes for automation
- Docker and direct-host connection modes
- Config bootstrap wizard (`init`) and connection check (`test`)
- Backup retention cleanup (`KEEP_DAYS`)
- Interactive database/schema drop, plus disconnect-without-drop (`drop`)
- Backup integrity verification (`verify`)
- Pre-flight diagnostics (`doctor`)
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
fuelrod-backup verify    [OPTIONS]
fuelrod-backup doctor    [OPTIONS]
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

Interactive restore wizard with engine-specific options. PostgreSQL includes TOC analysis for schema, table, and role filtering.

The wizard navigates the backup directory in four steps:

1. **Project directory** — pick a top-level folder under `BASE_DIR` (e.g. `akilimo`, `fuelrod`)
2. **Engine directory** — auto-resolved from `DB_TYPE` (e.g. `postgres`, `mariadb`) — no prompt
3. **Database** — pick the database folder within the engine directory
4. **Backup file** — pick from files in that database folder only

- `--no-interactive, -n` — skip the wizard; requires `--project` and `--db`
- `--project, -p TEXT` — project/group folder under `BASE_DIR`
- `--db, -d TEXT` — repeatable; database(s) to restore
- `--file, -f TEXT` — backup file name inside the database folder (default: latest by mtime)
- `--target-db TEXT` — restore into a different database name
- `--scope TEXT` — `full` | `schema` | `data` (PostgreSQL)
- `--clean / --no-clean` — drop existing objects first (default: clean)
- `--schema TEXT` — repeatable; restore only these schemas
- `--no-owner` — restore with `--no-owner --no-privileges`
- `--dry-run` — print the plan and exit without changing anything
- `--incremental-chain` — after restoring the chosen base, apply every newer `_inc` dump in order
- `--yes, -y` — required to actually execute a non-interactive restore
- `--all-engines, -a` — restore all engines sequentially (multi-engine config)
- `--docker / --no-docker`
- `--db-type, -t`
- `--config, -c PATH`

Without `--yes` a non-interactive run prints the plan and exits without touching the server.

```bash
# Preview a restore
fuelrod-backup restore -n -p fuelrod -d app --target-db app_restored --dry-run

# Run it unattended (cron/CI)
fuelrod-backup restore -n -p fuelrod -d app --target-db app_restored --yes
```

### `doctor`

Run pre-flight checks and report everything that is not ready yet.

- `--skip-connection` — skip the live database connection test
- `--config, -c PATH`

Checks the Python runtime, config file discovery, credentials, `BASE_DIR` existence/writability and
free disk space, retention settings, required external binaries (`pg_dump`, `pg_restore`,
`mariadb-dump`, `mysql`), Docker and service state, the database connection, and the `rclone`
remote plus n8n service list. Each failure prints a hint with the fix. Exits non-zero on failure.

### `verify`

Prove that backup files on disk are actually restorable. Exits non-zero if any artifact fails,
so it can gate a cron job or a restore drill.

- `--engine, -e TEXT` — restrict to one engine folder
- `--project, -p TEXT` — restrict to one project folder
- `--db, -d TEXT` — restrict to these database folders (repeatable)
- `--latest, -l` — verify only the newest backup per database
- `--checksum, -k` — also compute a SHA-256 for each artifact
- `--deep` — fully decompress and CRC-check every archive (slow on large trees)
- `--parallel, -j INT` — concurrent workers (default: `min(8, CPU count)`)
- `--json` — emit a machine-readable JSON document
- `--config, -c PATH`

Checks per type: gzip/zip integrity + SQL headers for `.sql`/`.sql.gz`/`.zip`, the `MICROSOFT`
media header for `.bak`, `pg_restore --list` for `.dump`/`.dump.gz` (no database connection
needed), and envelope validation for encrypted artifacts. Types that cannot be checked without an
external tool report `skip`, not failure.

By default `.sql.gz`/`.zip` archives are only probed at the header level — a full decompress of
every archive in a real backup tree (thousands of multi-GB files) is a `--deep` job. Artifacts are
checked in parallel with a progress bar.

### Incremental PostgreSQL backups

Set `INCREMENTAL=true` (PostgreSQL only) and the first backup of each database writes a full dump
plus a `.incremental_state.json` sidecar holding a per-table change counter. Later runs dump only
the tables whose counter moved, into `<db>_<timestamp>_inc.dump`; if nothing changed, no file is
written at all. Restoring requires base first, then the increments:

```bash
fuelrod-backup restore -n -p fuelrod -d app --file app_20260101_000000.dump     --incremental-chain --target-db app --yes
```

- The base of a chain is never pruned by `KEEP_DAYS`; only `_inc` files rotate, keeping the newest
  `INCREMENTAL_KEEP`.
- Change detection is DML-based (`pg_stat_user_tables` insert/update/delete counters). DDL-only
  changes to objects outside those tables are not captured — an increment is additive on top of
  its base, never a replacement. After a PostgreSQL stats reset every table looks changed, so the
  run degrades to a near-full dump rather than missing data.

### Encryption at rest

Set `ENCRYPT=true` and artifacts are encrypted after compression into `<file>.age` (or `.gpg`);
the plaintext is deleted only once the ciphertext exists, so a failed encryption never destroys a
backup. `backup --encrypt/--no-encrypt` overrides the config for one run. `doctor` reports whether
the tool, recipient and identity file are usable before you rely on it.

Restore detects the suffix automatically — interactive and `restore -n` both decrypt to a
temporary file that is removed when the process exits. Set `ENCRYPT_IDENTITY_FILE` (age) or
`ENCRYPT_PASSPHRASE_FILE` (gpg) on the machine that restores.

```bash
fuelrod-backup doctor --json | jq '.checks[] | select(.name=="Encryption at rest")'
fuelrod-backup backup -n --encrypt
fuelrod-backup restore -n -p fuelrod -d app --file app_20260101_000000.dump.gz.age --yes
```

### `drop`

Interactive database or schema drop. Terminates active connections before dropping and requires confirmation.

Use `--disconnect-only` to terminate connections **without** dropping anything.

- `--disconnect-only` — terminate all connections to the database; no data is deleted
- `--db, -d TEXT` — target database (skips the database prompt; mainly for `--disconnect-only`)
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

## Machine-readable output

`--json` is accepted globally and per command (`fuelrod-backup --json verify`, `fuelrod-backup
verify --json`). It silences all human output and writes a single JSON document to stdout;
diagnostics that are not part of the result (for example the auto-discovered config path) stay on
stderr, so stdout stays parseable. Every document carries a `command` and a `status` field.

```bash
fuelrod-backup doctor --json | jq '.summary'
fuelrod-backup verify --json --latest | jq '.artifacts[] | select(.status=="fail")'
```

Supported today: `backup`, `restore`, `verify`, `doctor` and `drop --disconnect-only`.

## Config

Config is loaded in this order:

1. Internal defaults
2. Config file (`--config` if passed, otherwise auto-discovered)
3. Environment variables (highest priority)

### Auto-discovery order

For each directory below, files are checked in this order: `.backup`, `.env`, `.env-backup`

Directories searched:

1. home directory (`~/`) — Linux/macOS only
2. current working directory
3. package directory
4. parent repo directory

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
| `INCREMENTAL` | `true`/`false` — dump only changed tables (PostgreSQL only) |
| `INCREMENTAL_KEEP` | number of `_inc` dumps to keep alongside the base (default 5) |
| `ENCRYPT` | `true`/`false` — encrypt artifacts after compression |
| `ENCRYPT_TOOL` | `age` (default) or `gpg` |
| `ENCRYPT_RECIPIENT` | age public key (`age1…`) or gpg recipient key |
| `ENCRYPT_IDENTITY_FILE` | age private key file, needed to restore encrypted backups |
| `ENCRYPT_PASSPHRASE_FILE` | passphrase file for symmetric gpg (never pass a passphrase on the CLI) |
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

Backups are written under a project folder, then engine type, then database name:

```
<BASE_DIR>/
  <project>/
    postgres/
      <database>/
        <database>_<YYYYMMDD_HHMMSS>.dump[.gz]
    mariadb/
      <database>/
        <database>_<YYYYMMDD_HHMMSS>.sql[.gz]
    mssql/
      <database>/
        <database>_<YYYYMMDD_HHMMSS>.bak
```

The `<project>` folder is determined by how `BASE_DIR` is set per project. Set `BASE_DIR` to the root that contains all project folders and the restore wizard will let you navigate from there.

The restore wizard searches from `BASE_DIR`, not `BASE_DIR/<engine>`, so the directory does not need to exist at that path.

## Notes

- PostgreSQL backups use custom format (`.dump`). Compression wraps the dump in `.gz`.
- MariaDB backups use `.sql`. Restore accepts `.sql`, `.sql.gz`, and `.zip`.
- MSSQL backups use native `.bak` format (no gzip).
- Config files generated by `init` are written with `0o600` permissions.

## License

GNU General Public License v3.0 or later. See [LICENSE](LICENSE).

## Breaking Change

Single-engine mode uses engine-agnostic connection keys only:

- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`

The legacy single-engine keys `PG_USERNAME`, `PG_PASSWORD`, `PG_HOST`, `PG_PORT` are no longer loaded in single-engine mode. Use the engine-prefixed keys (`PG_*`, `MY_*`, `MS_*`) only in multi-engine mode together with `--all-engines`.
