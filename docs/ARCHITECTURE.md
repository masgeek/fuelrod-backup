# Architecture and Workflows

## High-level Modules

| Module | Responsibility |
|--------|---------------|
| `cli.py` | Typer app — all command entry points |
| `config.py` | Config parsing, three-layer merge, auto-discovery |
| `adapters/` | Engine implementations (postgres, mariadb, mssql) |
| `backup.py` | Backup wizard, execution, retention cleanup |
| `restore.py` | Restore wizard, TOC parsing, engine-specific restore |
| `verify.py` | Offline integrity checks for backup artifacts |
| `doctor.py` | Pre-flight diagnostics for config, tools, disk and connectivity |
| `output.py` | JSON output mode — silences Rich consoles, emits one document |
| `crypto.py` | Encryption at rest (age/gpg) with temp-plaintext lifecycle |
| `notify.py` | Webhook/command notifications and the shared exit-code contract |
| `runner.py` | PostgreSQL native + subprocess operations (psycopg3) |
| `drop.py` | Interactive database/schema drop |
| `create.py` | Interactive database/schema creation |
| `rclone_config.py` | Reads/writes `rclone.conf` remotes for the guided S3 setup in `sync-init` |
| `n8n_backup.py` | Hot Docker volume snapshots for n8n |
| `n8n_restore.py` | Volume archive restore for n8n |
| `storage.py` | Push (`sync`): upload to one or several rclone remotes / local folders, `--latest`, prune, the rclone runner (`run_rclone_process`, verbose logging) |
| `pull.py` | Parallel download from an rclone remote, history, exclude, delete-after-pull |
| `pullhistory.py` | What was already pulled (path + size) and the last manifest fingerprint, kept in the user config folder |
| `manifest.py` | `BASE_DIR/manifest.json`, a global index of every backup, read by `pull` instead of listing folders |
| `quota.py` | Remote size monitoring and pruning of the oldest backups |
| `schedule.py` | cron / Task Scheduler entries for backup, pull, push, quota, verify, doctor |
| `settings.py` | Registry of every config key, used by `init` (and tests) |
| `engine_select.py` | Engine prompt shared by the interactive commands |
| `lock.py` | Per-engine run lock shared by backup and restore |
| `retention.py` | GFS retention profiles (`RETENTION=daily=7,weekly=4,…`) |
| `hooks.py` | `PRE_BACKUP_HOOK` / `POST_BACKUP_HOOK` |
| `catalog.py` | Backup listing (`list`) and the size-drop check |
| `restoretest.py` | `verify --restore-test`: restore the newest backups into a throwaway database |
| `fileutil.py` | gunzip helper, human-readable sizes, empty-folder removal |
| `procenv.py` | Minimal subprocess environments |
| `migrate/` | MariaDB → PostgreSQL migration subsystem (`report.to_dict()` feeds both `--report` and `--json`) |
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

### Incremental PostgreSQL dumps

`INCREMENTAL=true` enables a logical increment strategy behind the `supports_incremental` capability
flag. `PgRunner.table_change_signature()` returns `{table: n_tup_ins + n_tup_upd + n_tup_del}` from
`pg_stat_user_tables`; `backup.changed_tables()` diffs it against `.incremental_state.json` in the
database directory. No state (or a vanished base) means a full dump plus a new sidecar. Changes
produce `<db>_<ts>_inc.dump` via `backup_tables()` (`pg_dump -t`), applied on top of the base with
`pg_restore`. The counter only moves within a stats epoch, so a stats reset degrades to a larger
dump — never to a missing one. `prune_incremental()` rotates `_inc` files; the base is protected
from age-based retention via `_protected_bases()`.

## Restore Flow

1. Validate config and connection.
2. Select database directory and backup file from the backup tree.
3. Run engine-specific restore:
   - **PostgreSQL** — TOC analysis (`pg_restore -l`); optional schema/table/role filtering; `pg_restore`.
   - **MariaDB** — pipe `.sql` (or decompress `.sql.gz` / `.zip`) through the MySQL client.
   - **MSSQL** — T-SQL `RESTORE DATABASE` via pymssql; copies `.bak` into container with `docker cp` if needed.
4. Print restore summary and post-restore stats (PostgreSQL).

`restore.py` also exposes a non-interactive path (`run_restore(interactive=False)`, CLI `-n`) for
cron/CI: `--project` and `--db` replace the directory prompts, `--file` defaults to the newest
backup by mtime, schema/scope/role options replace the TOC analysis prompts, and `--yes` is
required to execute — without it the run only prints the plan. Both paths share `_prepare_target_db`
(drop-or-overlay + create) and `_perform_restore` (ensure schemas, run the engine restore, remap,
post-restore report).

## Drop Flow

1. List databases (or schemas for PostgreSQL).
2. Prompt for selection and require a confirmation token.
3. Terminate active connections (`pg_terminate_backend` for PostgreSQL).
4. Execute `DROP DATABASE` / `DROP SCHEMA ... CASCADE`.

`drop` also offers a third target, **Connections**, which stops after step 3 — every session on the
database is killed and nothing is deleted. Reachable non-interactively via
`drop --disconnect-only [--db NAME]`. Both paths require typing the database name to confirm.

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

## Notification Flow

`notify.notify()` is the single entry point. `NOTIFY_ON` selects events (`success` / `failure` /
`all`); `NOTIFY_URL` receives a JSON POST (scheme-restricted to http/https) and
`NOTIFY_HOOK_SUCCESS` / `NOTIFY_HOOK_FAILURE` are command templates with `{status}` / `{command}` /
`{exit_code}` / `{summary}` placeholders, executed list-form with a minimal env and a hard timeout.
Every failure mode — unreachable endpoint, HTTP error, timeout, bad template, missing binary — is
returned as a warning string and never raises, so a notification outage cannot fail a backup that
succeeded.

Exit codes live in `notify.py` and are the contract automation scripts read: 0 ok, 1 operational
failure, 2 usage, 3 verification failed, 4 aborted by the operator. Declining a confirmation or
pressing Ctrl+C exits 4, not 0.

## Encryption Flow

`crypto.py` encrypts after compression and before the incremental state is written, so the recorded
base is the ciphertext that actually survives. `encrypt_file()` only unlinks the plaintext after the
ciphertext is confirmed non-empty. Restore calls `decrypt_if_needed()`, which decrypts to a temp file
registered with an `atexit` cleanup — plaintext dumps never accumulate in the temp directory.

The subprocess environment is built from an allowlist (`_ALLOWED_ENV`: PATH, HOME/USERPROFILE,
APPDATA, GNUPGHOME, XDG_*) rather than inherited: the tools need their own state directory, but the
parent environment would leak database passwords. Key material is never passed on the command line —
age takes a recipient public key and an identity file, gpg a recipient key or a passphrase file.

## Output Mode

`output.py` owns machine-readable output. Every module-level `Console()` is created through
`register(console)`, so `enable_json(True)` can repoint all of them at a null sink; commands then
call `emit(payload)` once at the end. Only `backup`, `restore`, `verify`, `doctor` and
`drop --disconnect-only` emit structured payloads today. Anything a module logs outside the payload
must go to stderr (`config.py` already does) so stdout stays parseable.

## Doctor Flow

`doctor.py` runs independent checks and reports `ok` / `warn` / `fail` / `skip` for each, with a
remediation hint attached to anything not `ok`. It checks the runtime, config discovery,
credentials, `BASE_DIR` writability and free disk, retention sanity, per-engine host binaries,
Docker plus service state, a live connection, the `rclone` remote and the n8n service list. Only
`fail` produces a non-zero exit code, so `doctor` can gate a job without warning noise blocking it.

## Verify Flow

`verify.py` walks `BASE_DIR` (canonical `<project>/<engine>/<database>/` layout, with a flat
fallback scan) and applies per-artifact checks that need no database connection:

| Artifact | Checks |
|----------|--------|
| `.dump` / `.dump.gz` | gzip integrity, then `pg_restore --list` object count (skipped if `pg_restore` is absent) |
| `.sql` / `.sql.gz` | gzip integrity, SQL header shape |
| `.zip` | CRC of every entry via `testzip()` |
| `.bak` | `MICROSOFT` media header (warns; `RESTORE VERIFYONLY` not run offline) |
| `.age` / `.gpg` | envelope header only |

Statuses are `ok` / `warn` / `skip` / `fail`; only `fail` sets a non-zero exit code, so an
unverifiable artifact never blocks a pipeline.

## n8n Volume Backup/Restore Flow

**Backup** — for each n8n service, `docker exec` + `tar` creates a `.tar.gz` of the volume contents. The container keeps running throughout.

**Restore** — extracts the archive into the volume with `docker cp` + `tar`; `--dry-run` prints the plan without touching files.

## Push Flow (`sync` / `push`)

1. Resolve the destinations: `--remote` (repeatable) or `PUSH_REMOTE` (`|` separated) or `REMOTE` / `S3_*`. A
   destination is an rclone remote or a local folder; Windows `D:/…` paths are folders, not remotes.
2. Select the files under `BASE_DIR` (`--include`, `--schema`, `--since/--until`) and keep only the newest
   `--latest N` (`PUSH_LATEST`, default 3) per database, with their manifests and the full base of a selected
   incremental (`storage.select_latest`). The selection is passed to rclone with `--files-from`.
3. Upload to each destination (`_sync_one`). Local destinations are not throttled and must be outside the backup folder.
4. Only after **every** destination succeeded are the uploaded local files deleted (`PUSH_DELETE_LOCAL`), then
   emptied folders are removed and the local manifest is rewritten. One failing destination does not stop the others
   (exit code 1, local files kept).
5. Optional pruning on each destination: `--days N` (age) and/or the quota (below). Nothing is pruned by default.
6. After uploading and pruning, a fresh `manifest.json` is published on the destination.

`rclone` is always started with list-form argv and an allowlisted environment (`rclone_env()`); `-v/-vv`
(`run_rclone_process`) echoes each command with credentials masked and streams rclone's log; the provider
flags (`provider_args()`) are a data change, not a code change.

## Pull Flow (`pull`)

1. Resolve the remote (`--remote`, `PULL_REMOTE`, `REMOTE`), then read `manifest.json` from it with one request.
   If its fingerprint equals the one stored after the last complete, unfiltered pull, stop (`NOTHING NEW`).
2. Build one job per `<engine>/<database>` from the manifest — or, when there is none, by listing the folders (only
   `postgres`/`mariadb`/`mssql` folders are pulled). Jobs run in a thread pool (`--parallel`).
3. Per job, decide which files to download: skip `--exclude` / `PULL_EXCLUDE` matches, files already local with the
   same size and files recorded in the pull history (so a local copy removed by `push` is not fetched again);
   `--overwrite` / `--ignore-history` change that. The list goes to `rclone copy --files-from-raw`. If a
   manifest-derived download fails, the folder is listed for real and retried once.
4. `--delete-remote` (needs `--yes`) then deletes only server files that are in the local folder with the same
   size (optionally `rclone check`), keeping the newest `--keep-remote` per database and the folders themselves;
   the server manifest is refreshed afterwards.
5. A complete, unfiltered, non-dry run records the manifest fingerprint in the history.

## Manifest

`manifest.py` maintains `BASE_DIR/manifest.json`: `{"version": 1, "fingerprint", "files": [{path, size, mtime}]}`.
The fingerprint covers path, size and mtime (to the second). It is rewritten after a backup run (and its
retention cleanup), after `push` removes local files, and — on destinations — after every push, `quota --prune`
and `pull --delete-remote`. A manifest that does not parse or does not match its own fingerprint is ignored.
Anything that changes the backup tree must refresh it. `MANIFEST=false` disables writing.

## Quota Flow (`quota`, `sync --quota`)

`quota.py` lists a destination once (`rclone lsjson -R`), sums the sizes and compares with `PUSH_QUOTA`. With
`--prune` it plans deletions oldest-first in units (dump + manifest), never touching the newest
`PUSH_QUOTA_KEEP` backups of each database, until usage is at `PUSH_QUOTA_TARGET` percent. A quota too small for
the protected backups is reported (exit 1), never "fixed" by deleting them.

## Scheduling

`schedule.py` renders one entry per job (`# fuelrod-backup:<job>` in cron, task `fuelrod-backup-<job>` on
Windows), so jobs are installed, listed and removed independently. The scheduled command always uses
`--no-interactive --json` and absolute paths for the executable and config file.

## Locking

`lock.py` holds `BASE_DIR/.fuelrod-<engine>.lock` for the duration of a backup or restore of that engine, so they never
overlap. A stale lock (dead pid on this host, or older than 24 h) is reclaimed.

## Init and the Settings Registry

`settings.py` has one `Setting` per config key (type, default, group, help, validation, dependencies, owning command).
`init --list` prints it, `init --section/--advanced` walks the groups, and tests fail when `config.py` reads a key
without an entry or a key is missing from `.backup.example`. Config writers merge (`cli._write_config` /
`_update_config_keys`): they update only the keys a wizard manages and keep every other line, saving a `.bak` copy.

## PostgreSQL Runner

`runner.py::PgRunner` handles all PostgreSQL operations using psycopg3 as the native driver. Subprocesses are used **only** for `pg_dump`, `pg_restore`, and `docker cp`. All subprocess calls pass an explicit minimal `env` dict — the parent environment is never inherited — to prevent credential leakage.

## Config Loading

Three-layer merge: **internal defaults → config file → environment variables**.

Auto-discovery checks the user config directory first (`config.py::user_config_dir()` —
`%APPDATA%\fuelrod-backup` on Windows, `$XDG_CONFIG_HOME/fuelrod-backup` or `~/.config/fuelrod-backup`
elsewhere), then home, cwd, package dir, and repo root; each directory tries `.backup`, `.env`, `.env-backup`.
`init` writes to `user_config_dir()` by default. Multi-engine mode loads per-engine configs via `PG_*`, `MY_*`, `MS_*` key prefixes using `load_all_configs()`.
