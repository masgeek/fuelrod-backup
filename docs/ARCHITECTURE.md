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
| `runner.py` | PostgreSQL native + subprocess operations (psycopg3) |
| `drop.py` | Interactive database/schema drop |
| `n8n_backup.py` | Hot Docker volume snapshots for n8n |
| `n8n_restore.py` | Volume archive restore for n8n |
| `gdrive_sync.py` | Google Drive upload via rclone |
| `migrate/` | MariaDB → PostgreSQL migration subsystem |
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

## Google Drive Sync Flow

1. Resolve backup files matching glob include patterns.
2. Upload to the configured rclone remote folder.
3. Prune remote files older than `--days` (default: 2 days).
4. Optionally delete local files after successful upload.

## PostgreSQL Runner

`runner.py::PgRunner` handles all PostgreSQL operations using psycopg3 as the native driver. Subprocesses are used **only** for `pg_dump`, `pg_restore`, and `docker cp`. All subprocess calls pass an explicit minimal `env` dict — the parent environment is never inherited — to prevent credential leakage.

## Config Loading

Three-layer merge: **internal defaults → config file → environment variables**.

Auto-discovery checks the user config directory first (`config.py::user_config_dir()` —
`%APPDATA%\fuelrod-backup` on Windows, `$XDG_CONFIG_HOME/fuelrod-backup` or `~/.config/fuelrod-backup`
elsewhere), then home, cwd, package dir, and repo root; each directory tries `.backup`, `.env`, `.env-backup`.
`init` writes to `user_config_dir()` by default. Multi-engine mode loads per-engine configs via `PG_*`, `MY_*`, `MS_*` key prefixes using `load_all_configs()`.
