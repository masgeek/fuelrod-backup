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
- Off-site sync to Google Drive, S3, B2, SFTP or any rclone remote (`sync`), and parallel download back to local storage (`pull`)
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
fuelrod-backup sync        [OPTIONS]
fuelrod-backup pull        [OPTIONS]
fuelrod-backup pull-init   [OPTIONS]
fuelrod-backup push        [OPTIONS]   (alias of sync)
fuelrod-backup push-init   [OPTIONS]
fuelrod-backup quota       [OPTIONS]
fuelrod-backup schedule    [OPTIONS]
fuelrod-backup sync [OPTIONS]  Upload backups to an off-site remote via rclone (S3, B2, etc.)
```

The short alias `fr-bkp` is equivalent to `fuelrod-backup` for all commands.

### `init`

Wizard to generate a `.backup` config file.

Re-running it on an existing file is safe: the wizard only updates the keys it asks about (in place) and keeps every
other line — `RETENTION`, `NOTIFY_*`, `ENCRYPT_*`, `PULL_*` / `PUSH_*`, hooks, your comments. The previous file is saved
as `.backup.bak`. The one exception is deselecting an engine in multi-engine mode, which removes that engine's
connection keys. `sync-init`, `pull-init` and `push-init` merge the same way.

- `--output, -o PATH` — output path (default: `.backup`)
- `--list, -l` — list **every** setting with its current value, default, description and the command that sets it
  (`--json` for machines). Passwords are never shown
- `--section, -s NAME` — repeatable (or `all`): configure only that group and skip the connection wizard. Groups:
  `storage` (retention, manifest), `backup` (incremental, size warning, hooks), `encryption`, `notifications`, `push`,
  `quota`, `pull`, `rclone`, `n8n`
- `--advanced` — skip the connection wizard and pick the groups to review from a list

The main wizard ends by offering the pull/push remotes and the advanced settings, so every key of `.backup.example` can be
set from `init` (`pull-init`, `push-init` and `sync-init` cover the remotes). Settings that only matter when another one is
on (for example `ENCRYPT_*` when `ENCRYPT=false`) are skipped; answers are validated; values equal to the default are not
written; clearing an answer removes the key; everything else in the file is left alone.

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

- `--no-interactive, -n` — skip the wizard; requires `--db` (and `--project` only if backups sit under a project folder)
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
- `--json` — emit the migration report on stdout instead of the summary table
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

### `quota`

Show how much of a remote quota your backups use, and optionally prune the oldest ones. Usage is the total
size of the files under the remote path, compared with a limit you set (`PUSH_QUOTA=50G`); if the provider
reports its own total/used/free (`rclone about`), that is shown too.

- `--remote, -r TEXT` — destination to measure: an rclone remote or a local folder; repeatable (default `PUSH_REMOTE`, then
  `REMOTE` / `S3_*`). Each destination is checked against the quota separately
- `--quota, -q SIZE` — limit such as `500M`, `50G`, `1T` (default: `PUSH_QUOTA`)
- `--prune` — delete the oldest backups until usage is at `--target` percent of the quota (default 90, or
  `PUSH_QUOTA_TARGET`). A backup's dump and manifest are deleted together
- `--keep INT` — newest backups of each database that are never deleted (default 1, or `PUSH_QUOTA_KEEP`);
  if the quota is too small to fit them, pruning stops and the command reports it instead
- `--dry-run, -n` — with `--prune`, show what would be deleted
- `--json` — machine-readable output
- exits 1 while the remote is over quota (and sends the usual failure notification), so cron or a monitor can alert on it

With `PUSH_QUOTA` set, `sync` / `push` enforces the same limit on every destination after the upload (`sync --quota 50G`
overrides it).

```bash
fuelrod-backup quota -r akilimo-bkp:db-backup -q 50G            # report
fuelrod-backup quota -r akilimo-bkp:db-backup -q 50G --prune -n # preview the prune
fuelrod-backup push -q 50G                                      # upload, then prune to fit
```

### `schedule`

Schedule runs of `backup`, `pull`, `push`, `quota`, `verify` or `doctor` with cron (Linux/macOS) or Windows Task
Scheduler. Each job has its own entry, so a backup, a push and a pull can all be installed and are replaced
independently. Without `--apply` it only prints the crontab line / `schtasks` command.

- `--job, -j NAME` — `backup` (default), `pull`, `push`, `quota`, `verify`, `doctor`
- `--cron EXPR` — five-field cron expression. Defaults per job: backup `0 2 * * *`, pull `30 3 * * *`, push `0 4 * * *`,
  quota `0 5 * * *`, verify `30 5 * * *`, doctor `0 6 * * 1`. On Windows daily, every-N-hours (`0 */6 * * *`) and weekly
  (`30 3 * * 1,5`) schedules are supported
- `--arg, -a TEXT` — repeatable; extra options for the job, e.g. `--arg "--latest 5"` or
  `--arg "--delete-remote --yes"` (a scheduled pull that deletes on the server needs `--yes`, so it is never implied)
- `--log PATH` — where the job appends its output (default: `logs/fuelrod-backup-<job>.log` in your config folder)
- `--apply` — install it; `--list` shows the installed jobs; `--remove` deletes the one named by `--job`
- `--db-type`, `--config, -c` — passed through to jobs that accept them. The installed command always uses
  `--no-interactive --json`, and the absolute path of `fuelrod-backup` and of the config file

```bash
fuelrod-backup schedule --job backup --cron "0 2 * * *" --apply
fuelrod-backup schedule --job pull   --cron "30 3 * * *" --apply
fuelrod-backup schedule --job push   --cron "0 4 * * *" --arg "--latest 3" --apply
fuelrod-backup schedule --list
```

Order matters if they share data: pull the server's backups first, push to the off-site destinations afterwards.

### `pull`

Download backups from an rclone remote into local storage, in parallel. It expects the layout `sync`
uploads, `<remote>/<engine>/<database>/<files>`, and runs one rclone job per engine/database folder,
with `--parallel` jobs at a time. It never deletes anything locally. A file is downloaded only if it is neither in the
local folder (same size) nor in the **pull history** — a small record of what was already pulled, kept in your config
folder (`pull-history.json`). So if `push` (or you) removes the local copy, the next `pull` does not download it again.

- `--remote, -r TEXT` — source rclone remote, e.g. `akilimo-bkp:db-backup`. Point it at the folder that
  contains the `<engine>/` folders: only `postgres`, `mariadb` and `mssql` folders are pulled, and `pull`
  refuses to run on a directory that has none (use `--engine NAME` to override).
- `PULL_REMOTE` (config) — the remote to pull from by default, so a plain `pull` works (cron-friendly). Order:
  `--remote`, then `PULL_REMOTE`, then `REMOTE` / `S3_*`. `PUSH_REMOTE` is the same for `sync` / `push`.
- `pull-init` / `push-init` — pick one of your configured rclone remotes (`rclone listremotes`), enter the path
  inside it, and optionally test the connection. They write `PULL_REMOTE` / `PUSH_REMOTE` to your config file,
  and `init` offers to run them at the end.
- `--rclone PATH` / `RCLONE_CMD` (config) — rclone executable to use when it is not on `PATH`, e.g. `D:/tools/rclone.exe`.
- `--dest PATH` — local destination (default: `BASE_DIR`)
- `--engine, -e` / `--db, -d` — repeatable; only pull these engine / database folders
- `--include, -i GLOB` — repeatable; rclone glob to include, e.g. `*.dump.gz`
- `--exclude, -x GLOB` — repeatable; files that are never downloaded. A glob may match the file name (`*_inc.*`,
  `manifest_*.txt`), its path in the database folder, or the full `engine/database/file` path (`postgres/sonar/*` skips a
  whole database). `PULL_EXCLUDE` in config holds a default list, separated by `|`. Excluded files are also never deleted
  on the server by `--delete-remote`
- `--since INT` — only files modified in the last N days
- `--parallel, -j INT` — folder jobs at the same time (default 4)
- `--transfers INT` / `--checkers INT` — parallelism inside each job (default 4 / 8)
- `--bwlimit TEXT` — bandwidth limit per job, e.g. `10M`
- `--overwrite` — download everything again, replacing local files (ignores the history)
- `--no-manifest` — do not read `manifest.json` from the server; list its folders instead (see **Manifest** below)
- `--ignore-history` — also fetch files that were pulled before but are no longer in the local folder
- `--delete-remote` / `--no-delete-remote` — after pulling, **delete the files on the server** that are verified to be in
  the local folder with the same size (default `PULL_DELETE_REMOTE`, false). Never touches a file whose download failed
  or that is missing locally, and keeps the database folders themselves. Needs `--yes` (or a prompt in a terminal);
  `--dry-run` previews it
- `--keep-remote N` — with `--delete-remote`, leave the newest N backups of each database on the server (default
  `PULL_KEEP_REMOTE`, 0)
- `--verify` — with `--delete-remote`, also compare checksums (`rclone check`) before deleting
- `--yes, -y` — confirm deleting server files without asking
- `--verbose, -v` — show every rclone command (credentials masked) and its log live: each file copied, skipped or
  deleted, plus transfer stats every 5 s. `-vv` adds rclone's debug output and, for `pull`, the per-file decision
  (downloaded / already local / pulled before). In `--json` mode the extra output goes to stderr
- `--dry-run, -n` — show what would be downloaded
- `--json` — machine-readable output; exits 1 if any job fails

```bash
fuelrod-backup pull -r s3:acme-backups/prod -j 6
fuelrod-backup pull -r s3:acme-backups/prod -e postgres -d billing --since 7
fuelrod-backup pull -r akilimo-bkp:db-backup --dest D:/db-backup -j 6
```

### `sync`

Upload backup files to any rclone remote, with optional age-based remote pruning. Local files are
deleted only after a successful upload.

- `--remote, -r TEXT` — destination: an rclone remote (`s3:my-bucket/backups`, `b2:bucket`, `sftp:host:/srv/backups`)
  **or a local folder** (`D:/OneDrive/Backups`, a network share, a USB drive). Repeat it to push to several
  destinations at once; `PUSH_REMOTE` holds the default list, separated by `|`:
  `PUSH_REMOTE=akilimo-bkp:backups|D:/OneDrive/Backups`. Every destination receives the same files, local files are
  deleted only after all uploads succeeded, and one failing destination does not stop the others (exit code 1).
  With several destinations the interactive date/schema selection is skipped. A local destination must be outside
  the backup folder
- `--list-remotes` — list the rclone remotes configured on this host and exit
- `--dry-run, -d` — show plan without uploading
- `--days, -n INT` — prune remote files older than N days. Off by default; the old default of 2 days deleted
  freshly uploaded backups that were older than that, so it now only runs when you ask for it (or use a quota)
- `--include, -i GLOB` — repeatable; glob pattern to include
- `--keep-local` / `--delete-local` — keep or delete the local files after a successful upload (default: `PUSH_DELETE_LOCAL`,
  which is `true`). If you also `pull` into the same folder, keep them: deleted files are downloaded again by the next `pull`
  After deleting local files, folders that became empty are removed too (the backup folder itself stays)
- `--latest, -L INT` — upload only the newest N backups of each database, with their manifests (and the full base of
  any selected incremental dump). Default `PUSH_LATEST=3`; `0` uploads every file. Only the uploaded files are
  deleted locally.
- `PUSH_REMOTE` (config) — destination(s) to upload to by default, `|`-separated. `push` is an alias of `sync`, and
  `push-init` sets it interactively: pick rclone remotes or a local folder (for example your OneDrive folder), and
  add as many as you like (`init` offers it too).
- `--rclone PATH` — rclone executable to use
- `--verbose, -v` — show every rclone command (credentials masked) and its log live: each file copied, skipped or
  deleted, plus transfer stats every 5 s. `-vv` adds rclone's debug output and, for `pull`, the per-file decision
  (downloaded / already local / pulled before). In `--json` mode the extra output goes to stderr
- `--json` — machine-readable output
- `--config, -c PATH`

Backends: any remote rclone supports. Provider-specific flags are applied automatically (Google
Drive gets `--drive-use-trash=false` so pruning does not keep a second copy in the trash; S3 gets
`--s3-provider` when `S3_PROVIDER` is set for MinIO/Wasabi/etc.).

```bash
fuelrod-backup sync --list-remotes
fuelrod-backup sync -r s3:acme-backups/prod --keep-local
```
### `sync`

Upload backups to any rclone remote and prune old files there.

Works with every backend rclone supports: Google Drive, S3, S3-compatible
(MinIO, Wasabi), Backblaze B2, SFTP, Azure Blob, WebDAV. Local files are
deleted only after a successful upload.

## Manifest

A **global index of every backup** is kept at the root of the backup folder: `BASE_DIR/manifest.json`. It lists every
file of every engine (`path`, `size`, `mtime`) and a `fingerprint` of that list. Without it, anything that needs to know
"what is there?" has to walk the folders — on a remote (SFTP, S3, Drive) that is one request per folder, even when nothing
changed.

- `backup` rewrites it after each run (also after the retention cleanup), `push` rewrites it after deleting local files
  and **publishes a fresh copy on every destination** after uploading and pruning, and `quota --prune` and
  `pull --delete-remote` refresh the one they changed.
- `pull` reads that one file with a single request. It builds its jobs and file lists from it **without listing any
  folder**, and if the fingerprint is the same as at the end of the last complete, unfiltered pull it stops right there:
  `NOTHING NEW — no folders were listed`.
- A stale or damaged manifest is never trusted blindly: it must parse and match its own fingerprint, a failed download
  makes `pull` list that folder for real and retry, and `pull --no-manifest` always lists.
- Set `MANIFEST=false` to stop writing it. Hidden files (locks, incremental state) and the manifest itself are not listed.

## Notifications

Set `NOTIFY_ON` to `success`, `failure` or `all`, plus at least one destination:

```bash
NOTIFY_ON=failure
NOTIFY_URL=https://hooks.example.com/fuelrod
NOTIFY_HOOK_FAILURE=/usr/local/bin/backup-alert {status} {command} --summary "{summary}"
```

- The webhook receives one JSON POST (`command`, `status`, `summary`, `exit_code`, `engine`, `base_dir`).
  Only `http`/`https` are accepted.
- Hooks are command templates; `{status}`, `{command}`, `{exit_code}`, `{summary}` and scalar payload
  keys are substituted, and `FUELROD_STATUS` / `FUELROD_COMMAND` / `FUELROD_EXIT_CODE` are exported.
- Notification delivery is best-effort: a failed webhook or hook prints a warning and never changes
  the exit code of the run that triggered it.
- Wired into `backup`, `restore`, `verify` and `doctor`.

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | success |
| `1` | operational failure (connection refused, dump failed, bad config) |
| `2` | usage error (missing or unknown option) |
| `3` | `verify` found a broken artifact |
| `4` | aborted by the operator (declined confirmation, Ctrl+C) |

An operator abort is **not** success: it exits `4`, so automation can tell "nothing was done
because a human said no" from "everything worked".

## Machine-readable output

`--json` is accepted globally and per command (`fuelrod-backup --json verify`, `fuelrod-backup
verify --json`). It silences all human output and writes a single JSON document to stdout;
diagnostics that are not part of the result (for example the auto-discovered config path) stay on
stderr, so stdout stays parseable. Every document carries a `command` and a `status` field.

```bash
fuelrod-backup doctor --json | jq '.summary'
fuelrod-backup verify --json --latest | jq '.artifacts[] | select(.status=="fail")'
```

Supported by every command that has a result worth reading: `backup`, `restore`, `verify`, `doctor`,
`create`, `drop --disconnect-only`, `n8n-backup`, `n8n-restore`, `sync` and `migrate`.

Errors never travel inside the JSON document — they go to stderr with the exit code, so stdout always
parses. Commands whose wizard normally picks the target require it explicitly under `--json`
(`n8n-restore --service --file`, `restore -n --project --db`).

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
| `NOTIFY_ON` | `success` / `failure` / `all` — which events notify |
| `NOTIFY_URL` | webhook endpoint receiving a JSON POST |
| `NOTIFY_HOOK_SUCCESS` | command run on success, e.g. `/bin/notify {status} {command}` |
| `NOTIFY_HOOK_FAILURE` | command run on failure |
| `NOTIFY_TIMEOUT` | webhook/hook timeout in seconds (default 15) |
| `S3_PROVIDER` | rclone `--s3-provider` (Minio, Wasabi, ...) for `s3:` remotes |
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
