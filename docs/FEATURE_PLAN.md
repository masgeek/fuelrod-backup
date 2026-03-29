# Feature Plan

## Goal

Expand `fuelrod-backup` from a solid backup/restore CLI into a reliable operations tool with stronger automation, observability, and safety.

## Prioritization Framework

Score each feature by:

- User impact
- Operational risk reduction
- Implementation effort
- Ongoing maintenance cost

Execution order should prefer high-impact, low/medium-effort items first.

## Phase 1: High Value, Low Risk

### 1. Non-interactive restore mode

Why:

- Enables automation and disaster-recovery scripts.
- Complements existing non-interactive backup mode.

Scope:

- Add `restore --no-interactive` with explicit required flags.
- Add safety flags like `--force`, `--target-db`, `--drop-existing`.

Acceptance:

- Full restore can run without prompts.
- Dry-run output prints exact restore command plan.

### 2. Structured logs and machine-readable output

Why:

- Easier CI/CD integration and auditability.

Scope:

- Add `--log-format text|json`.
- Add `--output json` for command summaries.

Acceptance:

- Backup/restore/test commands emit stable JSON schema when requested.

### 3. Pre-flight checks command

Why:

- Reduces runtime surprises.

Scope:

- New command: `fuelrod-backup doctor`.
- Checks binaries, docker availability, config validity, backup directory access.

Acceptance:

- Exit code indicates pass/fail.
- Prints actionable remediation hints.

## Phase 2: Reliability and Safety Enhancements

### 4. Backup verification command

Why:

- Detects corrupt/incomplete backups early.

Scope:

- New command: `verify` to validate backup files per engine.
- PostgreSQL: TOC readability and optional test-restore metadata check.
- MariaDB/MSSQL: basic structural verification.

Acceptance:

- Verification report for each backup file.
- Non-zero exit on failed verification.

### 5. Restore guardrails and policy mode

Why:

- Prevents destructive mistakes.

Scope:

- Add environment protections (`prod` lock, allowlist).
- Require confirmation token for destructive operations unless forced.

Acceptance:

- Destructive restore blocked by default in protected environments.

### 6. Locking and concurrency controls

Why:

- Prevents overlapping jobs from corrupting state.

Scope:

- Per-database lock file strategy with timeout.
- Configurable lock behavior (`wait`, `fail-fast`).

Acceptance:

- Simultaneous runs are deterministic and safe.

## Phase 3: Operational Integrations

### 7. Retention policy profiles

Why:

- Flexible rotation for production needs.

Scope:

- Replace simple `KEEP_DAYS` with optional policy profiles:
  - daily retention
  - weekly retention
  - monthly retention

Acceptance:

- Policy-driven cleanup works predictably across engines.

### 8. Storage backends (S3-compatible first)

Why:

- Offsite backups and disaster recovery.

Scope:

- Optional upload after backup to S3-compatible storage.
- Restore can fetch remote backup before execution.

Acceptance:

- Backup and restore workflows support remote mode end-to-end.

### 9. Notifications/hooks

Why:

- Better operations visibility.

Scope:

- Add webhook/on-success/on-failure hooks.
- Optional Slack/Teams generic webhook support.

Acceptance:

- Hook payload includes command, engine, database, duration, result.

## Cross-Cutting Work

- Expand automated test coverage for config parsing, command workflows, and adapter behavior.
- Add golden tests for JSON outputs.
- Add migration notes for breaking config changes.

## Suggested Delivery Sequence

1. Non-interactive restore
2. Structured logs + JSON output
3. `doctor` command
4. Verification command
5. Restore guardrails
6. Concurrency locks
7. Retention profiles
8. S3 backend
9. Notifications/hooks

## Backlog Ideas

- Incremental PostgreSQL backup strategy support.
- Encryption at rest for generated backup artifacts.
- Minimal web dashboard for backup catalog browsing.
- Pluggable adapter interface for additional engines.
