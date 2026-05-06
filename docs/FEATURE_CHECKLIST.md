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
- [x] Google Drive sync via rclone (`gdrive-sync`)
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

- [ ] 1. Non-interactive restore mode
- [ ] 2. Structured logs and machine-readable output
- [ ] 3. Pre-flight checks command (`doctor`)
- [ ] 4. Backup verification command (`verify`)
- [ ] 5. Restore guardrails and policy mode
- [ ] 6. Locking and concurrency controls
- [ ] 7. Retention policy profiles
- [ ] 8. Storage backends (S3-compatible first)
- [ ] 9. Notifications/hooks

## Optional Backlog

- [ ] Incremental PostgreSQL backup strategy support
- [ ] Encryption at rest for backup artifacts
- [ ] Minimal web dashboard for backup catalog browsing
- [ ] Pluggable adapter interface for additional engines
