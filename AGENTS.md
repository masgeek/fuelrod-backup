# AGENTS.md

Python 3.13+, Poetry-managed CLI (`fuelrod-backup` / short alias `fr-bkp`).

## Commands

```bash
poetry install                      # virtualenvs live in ./.venv (poetry.toml)
poetry run ruff check .             # lint — CI gate, must pass first
poetry run pytest -q                # 212 fast unit tests, no DB/docker required
poetry run pytest tests/test_migrate_types.py -q   # single file
poetry run pytest -k toc -q                        # single test by name
poetry run fuelrod-backup --help                   # CI smoke-tests the entry point
poetry build
```

No mypy/format tooling — ruff only. CI (`.github/workflows/pr-checks.yml`) runs `ruff check .` then `pytest`, then smoke-tests `--help` on Python 3.13. Test result XML lands at `test-results.xml` (gitignored).

## Architecture (non-obvious bits)

- `fuelrod_backup/cli.py` is the only Typer app; every command is a thin wrapper that loads config then calls a module function (`backup.py`, `restore.py`, `drop.py`, `create.py`, `n8n_*.py`, `gdrive_sync.py`, `migrate/__init__.py`).
- `adapters/` holds the engine implementations behind `DbAdapter` (`adapters/base.py`), selected by `adapters/__init__.py::get_adapter(cfg)`. **Add new engine capabilities as capability flags (`supports_schemas`, `supports_roles`, `supports_toc`, `supports_schema_drop`, `dump_extension`), not `if db_type == ...` branches** — wizards read those flags to decide which options to show.
- `runner.py::PgRunner` is the PostgreSQL workhorse: psycopg3 for all queries/admin ops, subprocess **only** for `pg_dump` / `pg_restore` / `docker cp`. `psql` is not used.
- `migrate/` is a self-contained MariaDB→PostgreSQL pipeline (types → transform → schema → data → validate → report); `__init__.py::run_migrate()` is the public entry.

## Hard rules

- **Never inherit the parent environment in subprocesses.** All `subprocess.run` calls for DB tools must pass an explicit minimal `env=` dict (`PgRunner._host_env()` or a local `env`). This is a credential-leakage guard, not a style choice.
- **Validate identifiers before building paths or SQL.** `_validate_identifier` (per adapter) / `_validate_path_component` (backup.py) / `_validate_resolved_path` (restore.py) exist because of past path-traversal and injection fixes (VUL-01..07). New user-supplied db/service/file names must go through them.
- Ruff `S` (bandit) rules are on. `S603`/`S607` are globally ignored; other S-rules need a per-line `# noqa: Sxxx — <reason>`. S608 is expected in `migrate/` and must stay justified inline.

## Config

- Three-layer merge: defaults → file → env vars (env wins).
- **Auto-discovery order changed recently** (`config.py::_find_config_file`): user config dir first — Windows `%APPDATA%\fuelrod-backup`, else `$XDG_CONFIG_HOME/fuelrod-backup` → `~/.config/fuelrod-backup` — then `~`, cwd, package dir, repo root. Each dir checked for `.backup`, `.env`, `.env-backup`. `init` writes to `user_config_dir()` by default.
- Single-engine mode uses engine-agnostic `DB_*` keys only. The `PG_*` / `MY_*` / `MS_*` prefixed keys apply **only** to multi-engine mode (`--all-engines`, `load_all_configs()`). Legacy `PG_USERNAME` etc. are ignored in single-engine mode.
- The repo has a real `.backup` at the root (gitignored) that the CLI will auto-discover during local runs — it can affect manual smoke tests.
- Tests use the `clean_env` / `no_config_console` fixtures in `tests/conftest.py`; new config tests must use `clean_env` so the developer's own env vars don't leak into assertions.

## Git / workflow

- Branches: `develop` is the integration branch, `main` is release. PRs target `main` or `develop`; PRs are auto-approved and build/release workflows fire off `workflow_run` on `PR Checks` success.
- Conventional Commits (`feat:`, `fix:`, `refactor:`, `chore:`, `test:`), and the repo uses `VUL-nn` / issue references in bodies.
- `docs/` holds architecture, configuration, migration guide, and feature plan/checklist — update `ARCHITECTURE.md` when module layout or flows change. `CLAUDE.md` is a sibling instruction file; it is partially stale (its config auto-discovery section predates the app-dir change and it omits the `create` command).