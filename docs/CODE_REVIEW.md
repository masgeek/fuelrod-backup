# Code Review Notes

Review date: 2026-03-29

## Key Findings Addressed

1. `--compress` default behavior now respects config unless explicitly overridden.
2. Config discovery now includes `.env` in addition to `.backup` and `.env-backup`.
3. PostgreSQL table restore filtering now passes schema-qualified table names.
4. Error message references were aligned with real options.
5. `.backup.example` now uses current supported keys.
6. README and package metadata now match the current multi-engine implementation.

## Remaining Risks

- Temporary files are created using `tempfile.mktemp` in several restore/adapter paths.
  Consider replacing with `NamedTemporaryFile` or `mkstemp` for safer temp-file handling.
- Test coverage is not currently validated in this repository snapshot.
