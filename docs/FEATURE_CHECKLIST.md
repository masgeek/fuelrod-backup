# Feature Checklist (Priority Order)

Use this as the execution tracker. Items are ordered from highest to lowest recommended priority.

## Shipped

- [x] Interactive backup wizard (all engines)
- [x] Interactive restore wizard with PostgreSQL TOC/schema/role filtering
- [x] Non-interactive backup mode (`--no-interactive`)
- [x] Multi-engine parallel backup / sequential restore (`--all-engines`)
- [x] Backup retention cleanup (`KEEP_DAYS`)
- [x] Progress bars for backup and restore
- [x] Interactive database and schema drop (`drop` command)
- [x] n8n Docker volume backup and restore
- [x] Off-site sync via rclone (`sync`)
- [x] MariaDB → PostgreSQL migration (`migrate` command)
  - [x] Schema extraction via `information_schema`
  - [x] DDL transformation (15+ rewrite rules)
  - [x] Batched data migration with retry and backoff
  - [x] Parallel table migration (ThreadPoolExecutor)
  - [x] Post-migration row-count validation
  - [x] Optional MD5 checksum validation
  - [x] Dry-run mode
  - [x] JSON migration report

## Planned

- [x] 1. Non-interactive restore mode (`restore -n -p PROJECT -d DB [--file] [--yes]`)
- [x] 2. Structured logs and machine-readable output (`--json`)
- [x] 3. Pre-flight checks command (`doctor`)
- [x] 4. Backup verification command (`verify`)
- [x] 5. Restore guardrails (`--force`, `--safety-dump`, active-connection check)
- [x] 6. Locking and concurrency controls (per-engine run lock)
- [x] 7. Retention policy profiles (`RETENTION=daily=7,weekly=4,…`)
- [x] 8. Storage backends (any rclone remote or local folder; several at once)
- [x] 9. Notifications/hooks (webhook, command hooks, pre/post backup hooks)

## Optional Backlog

- [x] Incremental PostgreSQL backup strategy support (`INCREMENTAL=true` + `restore --incremental-chain`)
- [x] Encryption at rest for backup artifacts (`ENCRYPT=true`, age/gpg)
- [ ] Minimal web dashboard for backup catalog browsing
- [ ] Pluggable adapter interface for additional engines

## Shipped after the original plan

- [x] `pull`: parallel download with history, `--exclude`, `--delete-remote`, `-v/-vv`
- [x] `push`: several destinations (remotes or local folders), `--latest`, `PUSH_DELETE_LOCAL`
- [x] `quota`: monitor and prune a destination to a size limit
- [x] Global `manifest.json` read by `pull` instead of listing folders
- [x] `schedule` for backup, pull, push, quota, verify, doctor
- [x] `init --list / --section / --advanced`, merge-instead-of-rewrite, a registry of every setting
- [x] `list` command, `verify --restore-test`, engine prompt, `doctor`/`test` for every configured engine
