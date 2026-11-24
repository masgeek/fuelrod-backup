# Configuration Reference

Config is loaded from three layers in this order (highest priority last):

1. Internal defaults
2. Config file (`--config` path, or auto-discovered)
3. Environment variables

## Auto-discovery

On startup the tool walks these directories in order, checking each for `.backup`, `.env`, and `.env-backup`:

1. Current working directory
2. Project directory (package root)
3. Parent repo directory

The first file found is used. Pass `--config PATH` to bypass discovery entirely.

## Core Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DB_TYPE` | ✓ | `postgres`, `mariadb`, or `mssql` |
| `DB_USERNAME` | ✓ | Database login user |
| `DB_PASSWORD` | ✓ | Database login password |
| `DB_HOST` | ✓ | Hostname or IP address |
| `DB_PORT` | ✓ | Port number |
| `USE_DOCKER` | | `true`/`false` — route connections through a Docker container |
| `SERVICE` | | Container name (required when `USE_DOCKER=true`) |
| `BASE_DIR` | | Backup root directory (defaults to `<config-dir>/db-backup`) |
| `COMPRESS_FILE` | | `true`/`false` — gzip backup output (default: `false`) |
| `KEEP_DAYS` | | Retain backups for N days; `0` keeps everything (default: `7`) |
| `CONNECTION_TIMEOUT` | | Connection timeout in seconds (default: `30`) |

## Settings Reference

Every key the loader reads, with its default and the command that sets it. `fuelrod-backup init --list`
prints the same table with your current values (passwords masked), and `init --section NAME` /
`--advanced` configure a group interactively. Re-running `init` only changes what you answer and keeps
every other line of the file. This table is generated from `fuelrod_backup/settings.py` and a test
fails when it is out of date; regenerate it with `python -m fuelrod_backup.settings`.

<!-- settings:start -->
| Key | Default | Description | Set with |
|-----|---------|-------------|----------|
| `DB_TYPE` | `postgres` | Database engine | `init` |
| `DB_USERNAME` | — | Database user | `init` |
| `DB_PASSWORD` | — | Database password | `init` |
| `DB_HOST` | `127.0.0.1` | Database host | `init` |
| `DB_PORT` | — | Database port | `init` |
| `USE_DOCKER` | `true` | Run the dump/restore tools inside a Docker container | `init` |
| `SERVICE` | — | Docker container name | `init` |
| `BASE_DIR` | — | Backup root directory | `init` |
| `COMPRESS_FILE` | `false` | Compress backups with gzip | `init` |
| `KEEP_DAYS` | `7` | Keep backups for N days (0 = forever) | `init` |
| `CONNECTION_TIMEOUT` | `30` | Connection timeout in seconds | `init` |
| `PG_DUMP_CMD` | `pg_dump` | pg_dump command | `init` |
| `PG_RESTORE_CMD` | `pg_restore` | pg_restore command | `init` |
| `PSQL_CMD` | `psql` | psql command | `init` |
| `MYSQL_DUMP_CMD` | `mysqldump` | mysqldump / mariadb-dump command | `init` |
| `MYSQL_CMD` | `mysql` | mysql client command | `init` |
| `MSSQL_BACKUP_DIR` | `/var/opt/mssql/backups` | SQL Server backup directory inside the container | `init` |
| `RETENTION` | — | GFS retention, e.g. daily=7,weekly=4,monthly=12 (overrides KEEP_DAYS; blank = use KEEP_DAYS) | `init --section storage` |
| `MANIFEST` | `true` | Keep manifest.json, an index of all backups that pull reads | `init --section storage` |
| `INCREMENTAL` | `false` | Incremental backups (PostgreSQL: dump only changed tables) | `init --section backup` |
| `INCREMENTAL_KEEP` | `5` | Incremental dumps to keep per base (only when `INCREMENTAL` is set) | `init --section backup` |
| `SIZE_WARN_RATIO` | `0.5` | Warn when a dump is smaller than this fraction of the previous one (0 = off) | `init --section backup` |
| `PRE_BACKUP_HOOK` | — | Command run before a backup; a non-zero exit aborts it | `init --section backup` |
| `POST_BACKUP_HOOK` | — | Command run after a backup; failure only warns | `init --section backup` |
| `ENCRYPT` | `false` | Encrypt backups at rest | `init --section encryption` |
| `ENCRYPT_TOOL` | `age` | Encryption tool (only when `ENCRYPT` is set) | `init --section encryption` |
| `ENCRYPT_RECIPIENT` | — | Recipient (age public key or gpg key id) (only when `ENCRYPT` is set) | `init --section encryption` |
| `ENCRYPT_IDENTITY_FILE` | — | Private key file used to decrypt (age identity) (only when `ENCRYPT` is set) | `init --section encryption` |
| `ENCRYPT_PASSPHRASE_FILE` | — | File holding a passphrase (gpg symmetric) (only when `ENCRYPT` is set) | `init --section encryption` |
| `NOTIFY_ON` | — | When to notify | `init --section notifications` |
| `NOTIFY_URL` | — | Webhook URL (JSON POST) (only when `NOTIFY_ON` is set) | `init --section notifications` |
| `NOTIFY_TIMEOUT` | `15` | Notification timeout in seconds (only when `NOTIFY_ON` is set) | `init --section notifications` |
| `NOTIFY_HOOK_SUCCESS` | — | Command run on success ({status} {command} {summary}) (only when `NOTIFY_ON` is set) | `init --section notifications` |
| `NOTIFY_HOOK_FAILURE` | — | Command run on failure (only when `NOTIFY_ON` is set) | `init --section notifications` |
| `PUSH_REMOTE` | — | Push destinations (rclone remotes / folders, | separated) | `push-init` |
| `PUSH_LATEST` | `3` | Newest backups per database a push uploads (0 = all) | `init --section push` |
| `PUSH_DELETE_LOCAL` | `true` | Delete local files after a successful push | `init --section push` |
| `PUSH_QUOTA` | — | Remote size limit, e.g. 50G (blank = no quota) | `init --section quota` |
| `PUSH_QUOTA_TARGET` | `90` | Prune down to this percent of the quota (only when `PUSH_QUOTA` is set) | `init --section quota` |
| `PUSH_QUOTA_KEEP` | `1` | Newest backups per database never pruned (only when `PUSH_QUOTA` is set) | `init --section quota` |
| `PULL_REMOTE` | — | rclone remote pull downloads from | `pull-init` |
| `PULL_EXCLUDE` | — | Globs of files pull never downloads, | separated | `init --section pull` |
| `PULL_DELETE_REMOTE` | `false` | Delete server files after they are verified locally | `init --section pull` |
| `PULL_KEEP_REMOTE` | `0` | Newest backups per database left on the server (only when `PULL_DELETE_REMOTE` is set) | `init --section pull` |
| `RCLONE_CMD` | `rclone` | rclone executable (name on PATH or full path) | `init --section rclone` |
| `REMOTE` | — | Generic rclone remote for sync | `sync-init` |
| `S3_REMOTE` | — | rclone remote name for S3 | `sync-init` |
| `S3_BUCKET` | — | S3 bucket | `sync-init` |
| `S3_PREFIX` | — | S3 key prefix | `sync-init` |
| `S3_PROVIDER` | — | S3 provider | `sync-init` |
| `S3_REGION` | — | S3 region | `sync-init` |
| `S3_ENDPOINT` | — | S3 endpoint | `sync-init` |
| `S3_ACCESS_KEY_ID` | — | S3 access key id | `sync-init` |
| `S3_SECRET_ACCESS_KEY` | — | S3 secret access key | `sync-init` |
| `N8N_SERVICES` | `n8n` | Docker services to snapshot with n8n-backup (space separated) | `init --section n8n` |
| `SKIP_SERVICES` | — | Services n8n-backup must skip (space separated) | `init --section n8n` |
<!-- settings:end -->

Value conventions: booleans are `true`/`false`; lists are `|` separated (`PULL_EXCLUDE`, `PUSH_REMOTE`) or space
separated (`N8N_SERVICES`); `PUSH_REMOTE` / `PULL_REMOTE` accept rclone remotes (`name:path`) or, for push, local
folders (`D:/OneDrive/Backups`).

## Engine-specific Variables

### PostgreSQL

| Variable | Default | Description |
|----------|---------|-------------|
| `PG_DUMP_CMD` | `pg_dump` | Path or name of the pg_dump executable |
| `PG_RESTORE_CMD` | `pg_restore` | Path or name of the pg_restore executable |
| `PG_CMD` | `psql` | Path or name of the psql executable |

### MariaDB / MySQL

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_DUMP_CMD` | `mariadb-dump` | Path or name of the dump executable |
| `MYSQL_CMD` | `mysql` | Path or name of the client executable |

### MSSQL

| Variable | Default | Description |
|----------|---------|-------------|
| `MSSQL_BACKUP_DIR` | — | Backup directory path inside the container (Docker mode) |

## Multi-engine Mode

To manage multiple engines from a single config file, prefix each variable with the engine shorthand:

| Prefix | Engine |
|--------|--------|
| `PG_` | PostgreSQL |
| `MY_` | MariaDB/MySQL |
| `MS_` | MSSQL |

Example:

```
PG_DB_TYPE=postgres
PG_DB_USERNAME=postgres_user
PG_DB_PASSWORD=secret
PG_DB_HOST=127.0.0.1
PG_DB_PORT=5432

MY_DB_TYPE=mariadb
MY_DB_USERNAME=root
MY_DB_PASSWORD=secret
MY_DB_HOST=127.0.0.1
MY_DB_PORT=3306
```

Use `backup --all-engines` or `restore --all-engines` to operate across all configured engines.

## Output Layout

```
<BASE_DIR>/
  postgres/<database>/<YYYYMMDD-HHMMSS>.dump[.gz]
  mariadb/<database>/<YYYYMMDD-HHMMSS>.sql[.gz]
  mssql/<database>/<YYYYMMDD-HHMMSS>.bak
```

## Security Notes

- Config files generated by `init` are written with `0o600` permissions (owner read/write only).
- Never commit `.backup` or `.env` files that contain plaintext passwords.
- All subprocess calls to database tools (`pg_dump`, `mysqldump`, etc.) use an explicit minimal `env` dict; the parent process environment is never inherited.
