# Fix Checklist

Issues identified by code review. Ordered by priority.

---

## Critical Bugs

- [x] **`backup.py:166` — MSSQL `.bak` files incorrectly compressed**
  - Replace `not dump_file.suffix == ".bak"` with `dump_file.suffix != ".bak"`
  - Current code silently produces corrupt gzip-wrapped `.bak` files when `COMPRESS_FILE=true`

---

## Security

- [ ] **`adapters/mssql.py` — SQL injection in multiple methods**
  - Affected: `db_exists`, `get_db_size`, `terminate_connections`, `drop_db`, `backup_db`, `restore_db`
  - Use parameterised queries for `SELECT`/`COUNT` statements
  - For DDL (`BACKUP`/`RESTORE`/`DROP`/`CREATE`) which cannot be parameterised: validate `dbname` against `[A-Za-z0-9_\-]` before interpolating

- [ ] **`adapters/mariadb.py:278` — SQL injection in `terminate_connections`**
  - `dbname` is interpolated into `SELECT ID FROM information_schema.PROCESSLIST WHERE DB = '{dbname}'`
  - Pass `dbname` via the `params` tuple instead (already supported by `_query_col` elsewhere in the file)
  - Also audit `drop_db`/`create_db` backtick quoting — still vulnerable to names containing a backtick

- [ ] **`restore.py:461` — Full host environment leaked to Docker subprocess**
  - `env = None` causes `subprocess.run` to inherit the entire parent environment
  - Replace with `env={}` or `env={"PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin")}`
  - Also audit `runner.py:149,157` for the same pattern

- [ ] **`n8n_restore.py:288` — Path traversal via `tarfile.extractall`**
  - Add `filter='data'` argument (Python 3.12+): `tf.extractall(temp_dir, filter='data')`
  - Prevents `../` archive entries from escaping the temp directory

- [ ] **`cli.py:312` — `.backup` config file written without restricted permissions**
  - Add `output.chmod(0o600)` after writing the config file
  - Emit a warning that the file contains a plaintext credential

---

## Logic Bugs

- [x] **`n8n_backup.py:62` — Docker container name matched as substring**
  - `service in result.stdout` matches `n8n` inside `n8n-worker`
  - Replace with exact line match:
    ```python
    return any(line.strip() == service for line in result.stdout.splitlines())
    ```

- [x] **`restore.py:136–148` — `_parse_owners_from_toc` uses `parts[-1]` instead of `_split_toc_line`**
  - Multi-word TOC type names (e.g. `TABLE DATA`) shift columns, making `parts[-1]` the wrong token
  - Refactor to reuse `_split_toc_line` the same way `_parse_schemas_from_toc` does

- [x] **`restore.py:315–346` — `role_exists`/`create_role` called on base `DbAdapter`**
  - These methods are only on `PostgresAdapter` but called on the abstract base type
  - Declare them as optional methods on `DbAdapter`, or assert `isinstance(adapter, PostgresAdapter)` at the call site

---

## Minor

- [x] **`backup.py:188–193` — Empty directories left after retention cleanup**
  - After pruning all files in a `db_dir/dbname/` folder, remove the empty directory

- [ ] **`n8n_backup.py` — `alpine` image pulled silently on each backup**
  - Three `docker run --rm alpine` calls per backup with no local image check
  - Run `docker image inspect alpine` once at the start and warn if a pull is needed

- [ ] **`adapters/mariadb.py` — `_query_col` lacks a `params` argument**
  - Inconsistent with the PostgreSQL equivalent in `runner.py`
  - Add `params` support to prevent future callers from bypassing parameterisation
