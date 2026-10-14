"""Backup wizard and execution logic."""

from __future__ import annotations

import gzip
import json
import re
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Column, Table

from . import prompt as questionary
from .adapters import get_adapter
from .adapters.base import DbAdapter
from .config import Config
from .crypto import CryptoError, encrypt_file, validate_config
from .notify import EXIT_ABORTED, EXIT_FAILURE, EXIT_OK, notify, report_delivery_errors
from .output import emit, error, json_enabled, register

console = register(Console())


# ──────────────────────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _notify_finished(cfg: Config, command: str, dumped: list[dict]) -> None:
    """Send the success notification; delivery problems are warnings only."""
    changed = [d for d in dumped if d.get("status") == "ok"]
    summary = (
        f"{len(changed)} database(s) backed up"
        if not any(d.get("status") == "unchanged" for d in dumped)
        else f"{len(changed)} dumped, "
             f"{sum(1 for d in dumped if d.get('status') == 'unchanged')} unchanged"
    )
    errors = notify(
        status="success",
        command=command,
        cfg=cfg,
        summary=summary,
        exit_code=EXIT_OK,
    )
    report_delivery_errors(errors, console)


def _section(title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{title}[/]")
    console.print()


def _die(msg: str) -> None:
    if json_enabled():
        error(msg)
        sys.exit(EXIT_FAILURE)
    console.print(f"[bold red]ERROR:[/] {msg}")
    sys.exit(1)


_SAFE_PATH_RE = re.compile(r"^[A-Za-z0-9_\- ]+$")

# Sidecar written next to the dumps of a database when INCREMENTAL is enabled.
_STATE_FILE = ".incremental_state.json"

# Suffix marking a partial dump that only holds tables changed since the base.
_INCREMENT_SUFFIX = "_inc"


def _load_incremental_state(db_dir: Path) -> dict | None:
    """Read the incremental state sidecar, or None when absent/unreadable."""
    path = db_dir / _STATE_FILE
    if not path.is_file():
        return None
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(state, dict) or "signature" not in state:
        return None
    # The recorded base must still exist, otherwise fall back to a full dump.
    base = state.get("base")
    if base and not (db_dir / base).is_file():
        return None
    return state


def _save_incremental_state(db_dir: Path, state: dict) -> None:
    (db_dir / _STATE_FILE).write_text(json.dumps(state, indent=2), encoding="utf-8")


def changed_tables(previous: dict[str, int], current: dict[str, int]) -> list[str]:
    """
    Tables whose change counter moved, plus tables that appeared or vanished.

    A vanished table is deliberately excluded from the incremental dump: there
    is nothing to dump, and its rows stay in the base backup.
    """
    changed = [t for t, count in current.items() if previous.get(t) != count]
    return sorted(changed)


def prune_incremental(db_dir: Path, keep: int) -> list[Path]:
    """
    Delete all but the newest *keep* incremental dumps.

    The base dump is never removed here — an incremental dump is meaningless
    without it — so age-based retention must not be the only thing pruning
    this directory. Returns the removed paths.
    """
    increments = sorted(db_dir.glob(f"*_*{_INCREMENT_SUFFIX}.dump*"))
    removed: list[Path] = []
    for path in increments[:-keep] if keep > 0 else increments:
        try:
            path.unlink()
        except OSError:
            continue
        removed.append(path)
    return removed


def _protected_bases(backup_dir: Path) -> set[str]:
    """File names of every incremental chain base that must not be pruned."""
    protected: set[str] = set()
    if not backup_dir.is_dir():
        return protected
    for state_path in backup_dir.rglob(_STATE_FILE):
        state = _load_incremental_state(state_path.parent)
        base = state.get("base") if state else None
        if base:
            protected.add(base)
    return protected


def incremental_chain(db_dir: Path, base_file: Path) -> list[Path]:
    """
    Incremental dumps that belong on top of *base_file*, oldest first.

    Only dumps newer than the base are applied, so restoring an old base plus
    everything after it reproduces the state at the newest dump.
    """
    candidates = sorted(
        p for p in db_dir.glob(f"*{_INCREMENT_SUFFIX}.dump*")
        if p.name != base_file.name and p.stat().st_mtime > base_file.stat().st_mtime
    )
    return candidates


def _unique_dump_path(db_dir: Path, stem: str, ext: str) -> Path:
    """
    Pick a free dump path for *stem*+*ext*.

    Two backups within the same second would otherwise collide and the second
    would silently overwrite the first.
    """
    candidate = db_dir / f"{stem}{ext}"
    counter = 1
    while candidate.exists():
        candidate = db_dir / f"{stem}_{counter}{ext}"
        counter += 1
    return candidate


def _validate_path_component(name: str, label: str = "name") -> str:
    """Validate that *name* is safe to use as a filesystem path component."""
    if not name or not _SAFE_PATH_RE.match(name):
        _die(
            f"Invalid {label} '{name}': only letters, digits, underscores, "
            "hyphens, and spaces are permitted."
        )
    return name


# ──────────────────────────────────────────────────────────────────────────────
#  Interactive wizard
# ──────────────────────────────────────────────────────────────────────────────

def _wizard_connection(cfg: Config, adapter: DbAdapter) -> None:
    """Optionally override connection settings, then test."""
    _section("Connection")

    console.print(f"  Engine: [cyan]{cfg.db_type.value}[/]")
    if cfg.use_docker:
        console.print(f"  Mode  : [cyan]Docker[/] — service '[bold]{cfg.service}[/]'")
    else:
        console.print(f"  Mode  : Direct — {cfg.host}:{cfg.port}")
    console.print(f"  User  : {cfg.user}")
    console.print()

    if questionary.confirm("Override connection settings?", default=False).ask():
        if not cfg.use_docker:
            cfg.host = questionary.text("Host", default=cfg.host).ask() or cfg.host
            cfg.port = int(questionary.text("Port", default=str(cfg.port)).ask() or cfg.port)
        cfg.user = questionary.text("Username", default=cfg.user).ask() or cfg.user
        new_pass = questionary.password("Password (blank to keep current)").ask() or ""
        if new_pass:
            cfg.password = new_pass

    if not cfg.password:
        _die("Password is required. Set the appropriate *_PASSWORD variable in .backup.")

    console.print()
    try:
        questionary.check_connection_with_countdown(adapter.check_connection, cfg.connection_timeout)
    except TimeoutError as exc:
        _die(str(exc))
    console.print("[green]Connection OK.[/]")


def _wizard_databases(cfg: Config, adapter: DbAdapter) -> list[str]:
    """Let user pick which databases to back up."""
    _section("Select Databases")

    all_dbs = adapter.list_databases()
    if not all_dbs:
        _die("No databases found on server.")

    table = Table(show_header=True, header_style="bold")
    table.add_column("#", style="dim", width=4)
    table.add_column("Database", min_width=24)
    table.add_column("Size", justify="right")
    for i, db in enumerate(all_dbs):
        size = adapter.get_db_size(db)
        table.add_row(str(i), db, size)
    console.print(table)

    choices = [questionary.Choice(title=db, value=db) for db in all_dbs]
    selected = questionary.checkbox(
        "Select databases to back up (Space to toggle, Enter to confirm, none = all)",
        choices=choices,
    ).ask()

    if not selected:
        console.print("  No selection — backing up [bold]all[/] databases.")
        return all_dbs
    return selected


def _wizard_options(cfg: Config) -> None:
    """Override compress / keep-days / base_dir."""
    _section("Backup Options")

    cfg.compress = questionary.confirm(
        "Compress output with gzip?", default=cfg.compress
    ).ask()

    days_str = questionary.text(
        "Keep backups for N days (0 = forever)",
        default=str(cfg.days_to_keep),
    ).ask()
    try:
        cfg.days_to_keep = int(days_str or cfg.days_to_keep)
    except ValueError:
        pass

    # Show the base root (suffix /<db_type> is appended automatically via backup_dir)
    raw_base = questionary.text(
        "Output directory (/<db_type> suffix appended automatically)",
        default=str(cfg.base_dir),
    ).ask() or str(cfg.base_dir)

    # Store only the raw root; suffix is always applied via backup_dir property
    cfg.base_dir = str(Path(raw_base))


# ──────────────────────────────────────────────────────────────────────────────
#  Backup one database
# ──────────────────────────────────────────────────────────────────────────────

def _backup_one(
        db: str,
        cfg: Config,
        adapter: DbAdapter,
        *,
        progress=None,
        task_id=None,
) -> Path | None:
    """
    Dump a single database. Returns the final dump file path.

    Returns None when an incremental backup found nothing to dump, so an
    unchanged database costs nothing.
    """

    def _phase(desc: str) -> None:
        if task_id is not None:
            progress.update(task_id, description=desc)

    _validate_path_component(db, "database name")
    db_dir = Path(cfg.backup_dir) / db
    db_dir = db_dir.resolve()
    if not str(db_dir).startswith(str(Path(cfg.backup_dir).resolve())):
        _die(f"Path traversal detected for database '{db}'")
    db_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ext = adapter.dump_extension

    incremental = cfg.incremental and adapter.supports_incremental
    state = _load_incremental_state(db_dir) if incremental else None
    tables: list[str] | None = None
    base_name: str | None = None

    if state is not None:
        current = adapter.table_change_signature(db)
        tables = changed_tables(state["signature"], current)
        base_name = state.get("base")
        if not tables:
            _phase(f"[dim]{db}[/]  [dim]no changes[/]")
            if task_id is None:
                console.print(f"  [dim]-[/] {db} [dim]— no table changes since the last backup[/]")
            return None

    suffix = f"{_INCREMENT_SUFFIX}{ext}" if tables is not None else ext
    dump_file = _unique_dump_path(db_dir, f"{db}_{timestamp}", suffix)
    manifest_file = db_dir / f"manifest_{timestamp}.txt"

    # Write manifest
    mode = "incremental" if tables is not None else "full"
    with manifest_file.open("w") as mf:
        mf.write(f"Database  : {db}\n")
        mf.write(f"Engine    : {cfg.db_type.value}\n")
        mf.write(f"Timestamp : {timestamp}\n")
        mf.write(f"Host      : {cfg.host}:{cfg.port}\n")
        mf.write(f"User      : {cfg.user}\n")
        mf.write(f"Docker    : {cfg.use_docker}\n")
        mf.write(f"Compressed: {cfg.compress}\n")
        mf.write(f"Encrypted  : {cfg.encrypt_tool if cfg.encrypt else 'no'}\n")
        mf.write(f"Mode      : {mode}\n")
        if base_name:
            mf.write(f"Base      : {base_name}\n")
            mf.write(f"Tables    : {len(tables)} changed\n")
            for t in tables:
                mf.write(f"  - {t}\n")

    saved_tables: list[str] | None = None
    if tables is not None:
        _phase(f"[cyan]{db}[/]  dumping {len(tables)} changed table(s)…")
        adapter.backup_tables(db, dump_file, tables)
        saved_tables = tables
    else:
        _phase(f"[cyan]{db}[/]  dumping…")
        adapter.backup_db(
            db,
            dump_file,
            include_schemas=[],
            exclude_schemas=[],
        )

    if cfg.compress and dump_file.suffix != ".bak":
        _phase(f"[cyan]{db}[/]  compressing…")
        gz_file = Path(str(dump_file) + ".gz")
        with dump_file.open("rb") as f_in, gzip.open(gz_file, "wb", compresslevel=9) as f_out:
            shutil.copyfileobj(f_in, f_out)
        dump_file.unlink()
        dump_file = gz_file

    # Encrypt after compression so the plaintext never leaves the host.
    if cfg.encrypt:
        _phase(f"[cyan]{db}[/]  encrypting ({cfg.encrypt_tool})…")
        try:
            dump_file = encrypt_file(dump_file, cfg)
        except CryptoError as exc:
            _die(str(exc))

    # State is written after compression so `base` points at the file that
    # actually survives on disk.
    if incremental:
        signature = adapter.table_change_signature(db)
        if signature:
            _save_incremental_state(db_dir, {
                "base": base_name if saved_tables is not None else dump_file.name,
                "timestamp": timestamp,
                "tables": saved_tables or [],
                "signature": signature,
            })
            prune_incremental(db_dir, cfg.incremental_keep)

    size = dump_file.stat().st_size
    human = f"{size / 1024 / 1024:.1f} MB" if size > 1024 * 1024 else f"{size / 1024:.1f} KB"

    if task_id is not None:
        progress.update(task_id, description=f"[green]✓ {db}[/]  [dim]{human}[/]", completed=1)
    else:
        console.print(f"  [green]✓[/] {dump_file.name}  ([dim]{human}[/])")

    return dump_file


# ──────────────────────────────────────────────────────────────────────────────
#  Cleanup old backups
# ──────────────────────────────────────────────────────────────────────────────

def _cleanup_old(base_dir: str, days: int, protect: set[str] | None = None) -> None:
    """
    Delete dumps older than *days*.

    *protect* holds file names that must survive regardless of age — the base
    of an incremental chain, whose increments are meaningless without it.
    """
    if days <= 0:
        return
    import time
    keep = protect or set()
    cutoff = time.time() - days * 86400
    base = Path(base_dir)
    for pattern in (
        "**/*.dump", "**/*.dump.gz", "**/*.sql", "**/*.sql.gz", "**/*.bak",
        "**/*.age", "**/*.gpg", "**/manifest_*.txt",
    ):
        for f in base.glob(pattern):
            if f.name in keep:
                continue
            if f.stat().st_mtime < cutoff:
                f.unlink()
                console.print(f"  [dim]Removed old backup: {f.name}[/]")
    # Remove empty per-database subdirectories left after file pruning
    for db_dir in base.iterdir():
        if db_dir.is_dir() and not any(db_dir.iterdir()):
            db_dir.rmdir()
            console.print(f"  [dim]Removed empty directory: {db_dir.name}[/]")


# ──────────────────────────────────────────────────────────────────────────────
#  Public entry point
# ──────────────────────────────────────────────────────────────────────────────

def run_backup(
        cfg: Config,
        *,
        interactive: bool = True,
        databases: list[str] | None = None,
        compress: bool | None = None,
        keep_days: int | None = None,
) -> None:
    """Main backup workflow.

    Schema filtering is not applied at dump time — always dumps the full
    database. For PostgreSQL, schema selection happens at restore time via
    pg_restore -n (driven by TOC analysis in the restore wizard).
    """
    adapter = get_adapter(cfg)

    if cfg.encrypt:
        try:
            validate_config(cfg)
        except CryptoError as exc:
            _die(str(exc))

    # Apply CLI overrides before wizard (wizard may further override)
    if compress is not None:
        cfg.compress = compress
    if keep_days is not None:
        cfg.days_to_keep = keep_days

    if interactive:
        console.print(Panel(f"[bold cyan]{cfg.db_type.value.upper()} Backup Wizard[/]", expand=False))

        _wizard_connection(cfg, adapter)

        selected_dbs = _wizard_databases(cfg, adapter)

        _wizard_options(cfg)

        # Summary + confirm
        _section("Summary")
        console.print(f"  Engine      : [cyan]{cfg.db_type.value}[/]")
        console.print(f"  Databases   : [bold]{', '.join(selected_dbs)}[/]")
        console.print(f"  Compress    : {cfg.compress}")
        console.print(f"  Retention   : {cfg.days_to_keep} days")
        console.print(f"  Output dir  : {cfg.backup_dir}")
        console.print()

        if not questionary.confirm("Proceed with backup?", default=True).ask():
            console.print("[yellow]Aborted by user.[/]")
            sys.exit(EXIT_ABORTED)

        dbs_to_backup = selected_dbs
    else:
        # Non-interactive path
        if not cfg.password:
            _die("Password is required. Set it in .backup.")

        try:
            questionary.check_connection_with_countdown(adapter.check_connection, cfg.connection_timeout)
        except TimeoutError as exc:
            _die(str(exc))
        console.print("[green]Connection OK.[/]")

        if databases:
            dbs_to_backup = databases
        else:
            dbs_to_backup = adapter.list_databases()
            if not dbs_to_backup:
                _die("No databases found.")

    if not cfg.base_dir:
        _die("BASE_DIR is not set. Add it to .backup/.env config.")

    Path(cfg.backup_dir).mkdir(parents=True, exist_ok=True)

    _section("Running Backup")

    n = len(dbs_to_backup)
    dumped: list[dict] = []
    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}", table_column=Column(min_width=44)),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        overall = progress.add_task(f"[bold cyan]0/{n} databases[/]", total=n)
        for i, db in enumerate(dbs_to_backup, 1):
            task_id = progress.add_task(f"[dim]{db}[/]", total=1, completed=0)
            try:
                dump_file = _backup_one(db, cfg, adapter, progress=progress, task_id=task_id)
                dumped.append({
                    "database": db,
                    "file": str(dump_file) if dump_file else None,
                    "size": dump_file.stat().st_size if dump_file else 0,
                    "status": "ok" if dump_file else "unchanged",
                })
            except subprocess.CalledProcessError as exc:
                progress.update(task_id, description=f"[red]✗ {db}[/]", completed=1)
                dumped.append({"database": db, "file": None, "size": 0,
                               "status": "fail", "error": f"exit code {exc.returncode}"})
                _die(f"Backup failed for '{db}': exit code {exc.returncode}")
            except Exception as exc:
                progress.update(task_id, description=f"[red]✗ {db}[/]", completed=1)
                _die(f"Backup failed for '{db}': {exc}")
            progress.update(overall, advance=1, description=f"[bold cyan]{i}/{n} databases[/]")

    _cleanup_old(str(cfg.backup_dir), cfg.days_to_keep, protect=_protected_bases(Path(cfg.backup_dir)))

    if json_enabled():
        emit({
            "command": "backup",
            "status": "ok",
            "engine": cfg.db_type.value,
            "backup_dir": str(cfg.backup_dir),
            "compressed": cfg.compress,
            "keep_days": cfg.days_to_keep,
            "databases": dumped,
        })
        _notify_finished(cfg, "backup", dumped)
        return

    _notify_finished(cfg, "backup", dumped)

    console.print()
    console.print(Panel("[bold green]BACKUP COMPLETE[/]", expand=False))


# ──────────────────────────────────────────────────────────────────────────────
#  Parallel multi-engine entry point
# ──────────────────────────────────────────────────────────────────────────────

def run_parallel_backup(
        configs: list[Config],
        *,
        databases: list[str] | None = None,
        compress: bool | None = None,
        keep_days: int | None = None,
) -> None:
    """Run non-interactive backups for every engine in *configs* in parallel.

    Each engine's output is prefixed with ``[engine]`` so interleaved lines
    remain readable.  A final summary table shows pass/fail per engine.
    """
    if len(configs) == 1:
        run_backup(configs[0], interactive=False, databases=databases, compress=compress, keep_days=keep_days)
        return

    def _backup_engine(
            cfg: Config,
            progress: Progress,
            task_id,
    ) -> tuple[str, bool, str]:
        engine = cfg.db_type.value

        def _phase(desc: str) -> None:
            progress.update(task_id, description=desc)

        try:
            if compress is not None:
                cfg.compress = compress
            if keep_days is not None:
                cfg.days_to_keep = keep_days

            if not cfg.password:
                return engine, False, "password not set — check config"

            adapter = get_adapter(cfg)
            _phase(f"[cyan]{engine}[/]  connecting…")
            try:
                questionary.check_connection_with_countdown(adapter.check_connection, cfg.connection_timeout)
            except TimeoutError as exc:
                return engine, False, str(exc)

            dbs_to_backup = databases or adapter.list_databases()
            if not dbs_to_backup:
                return engine, False, "no databases found"

            if not cfg.base_dir:
                return engine, False, "BASE_DIR not set"

            Path(cfg.backup_dir).mkdir(parents=True, exist_ok=True)

            n_dbs = len(dbs_to_backup)
            progress.update(task_id, total=n_dbs, completed=0)
            for i, db in enumerate(dbs_to_backup, 1):
                _phase(f"[cyan]{engine}[/]  [{i}/{n_dbs}] dumping {db}…")
                _backup_one(db, cfg, adapter)
                progress.advance(task_id)

            _cleanup_old(str(cfg.backup_dir), cfg.days_to_keep)
            return engine, True, ""

        except subprocess.CalledProcessError as exc:
            return engine, False, f"subprocess exit {exc.returncode}"
        except Exception as exc:
            return engine, False, str(exc)

    console.print(Panel(
        f"[bold cyan]Parallel backup — {len(configs)} engine(s): "
        f"{', '.join(c.db_type.value for c in configs)}[/]",
        expand=False,
    ))

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}", table_column=Column(min_width=44)),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        with ThreadPoolExecutor(max_workers=len(configs)) as pool:
            task_ids = {
                cfg: progress.add_task(f"[dim]{cfg.db_type.value}[/]  starting…", total=None)
                for cfg in configs
            }
            futures = {
                pool.submit(_backup_engine, cfg, progress, task_ids[cfg]): cfg
                for cfg in configs
            }
            results: list[tuple[str, bool, str]] = []
            for fut in as_completed(futures):
                engine, ok, err = fut.result()
                cfg_done = futures[fut]
                tid = task_ids[cfg_done]
                if ok:
                    progress.update(tid, description=f"[green]✓ {engine}[/]")
                else:
                    progress.update(tid, description=f"[red]✗ {engine}  {err}[/]", total=1, completed=1)
                results.append((engine, ok, err))

    all_ok = all(ok for _, ok, _ in results)
    console.print()
    for engine, ok, err in sorted(results):
        if ok:
            console.print(f"  [green]✓[/] {engine}")
        else:
            console.print(f"  [red]✗[/] {engine}: {err}")
            all_ok = False

    if json_enabled():
        emit({
            "command": "backup",
            "status": "ok" if all_ok else "fail",
            "mode": "parallel",
            "engines": [
                {"engine": engine, "status": "ok" if ok else "fail", "error": err}
                for engine, ok, err in sorted(results)
            ],
        })
        if not all_ok:
            sys.exit(1)
        return

    console.print()
    if all_ok:
        console.print(Panel("[bold green]ALL ENGINES COMPLETE[/]", expand=False))
    else:
        console.print(Panel("[bold red]SOME ENGINES FAILED — see output above[/]", expand=False))
        sys.exit(1)
