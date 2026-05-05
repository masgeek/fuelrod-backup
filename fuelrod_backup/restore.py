"""Interactive restore wizard."""

from __future__ import annotations

import gzip
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Column, Table

from . import prompt as questionary
from .adapters import get_adapter
from .adapters.base import DbAdapter
from .config import Config

console = Console()

_SYSTEM_SCHEMA_RE = re.compile(
    r"^(pg_catalog|information_schema|pg_toast|pg_temp.*|-|pg_)$"
)
# _SYSTEM_ROLE_RE = re.compile(r"^(-|pg_[a-z_]+)$")
_SYSTEM_ROLE_RE = re.compile(r'^(postgres|pg_[a-z_]+)$', re.IGNORECASE)

# Second words of compound pg_restore object types.
_TYPE_KEYWORDS = frozenset({"CONSTRAINT", "ACL", "DATA", "OWNED", "SET", "BY"})

# Backup file extensions browsed in the restore wizard
_BACKUP_EXTENSIONS = ("*.dump", "*.dump.gz", "*.sql", "*.sql.gz", "*.zip", "*.bak")


def _section(title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{title}[/]")
    console.print()


def _die(msg: str) -> None:
    console.print(f"[bold red]ERROR:[/] {msg}")
    sys.exit(1)


def _human_size(path: Path) -> str:
    size = path.stat().st_size
    if size > 1024 * 1024:
        return f"{size / 1024 / 1024:.1f} MB"
    return f"{size / 1024:.1f} KB"


# ──────────────────────────────────────────────────────────────────────────────
#  TOC parsing helpers (PostgreSQL only)
# ──────────────────────────────────────────────────────────────────────────────

def _split_toc_line(parts: list[str]) -> tuple[str, str, str, str] | None:
    """
    Parse a non-comment TOC line into (obj_type, schema, name, owner).

    TOC format: id; oid flags TYPE [subtype] SCHEMA [TABLE] NAME OWNER

    The owner is always the last token and the name is always second-to-last.
    Anchoring from the end correctly handles object types whose tag includes a
    table reference before the object name (CONSTRAINT, FK CONSTRAINT, INDEX,
    DEFAULT, TRIGGER, RULE) -- those extra tokens are simply ignored between
    the schema position and parts[-2].

    Compound types (TABLE DATA, FK CONSTRAINT, SEQUENCE OWNED BY, ...) have a
    keyword in parts[4] that is not a schema name; detect these via
    _TYPE_KEYWORDS to find the correct schema position.
    """
    # Minimum valid line: id oid oid TYPE schema name owner = 7 tokens.
    if len(parts) < 7:
        return None

    # Owner and name are always the last two tokens, regardless of how many
    # table-reference tokens appear between the schema and the object name.
    owner = parts[-1]
    name = parts[-2]

    if len(parts) > 4 and parts[4] in _TYPE_KEYWORDS:
        if len(parts) > 5 and parts[5] == "BY":
            obj_type = f"{parts[3]} {parts[4]} BY"
            schema = parts[6] if len(parts) > 6 else "-"
        else:
            obj_type = f"{parts[3]} {parts[4]}"
            schema = parts[5] if len(parts) > 5 else "-"
    else:
        obj_type = parts[3]
        schema = parts[4]

    return obj_type, schema, name, owner


def _iter_toc(toc: str):
    """Yield (obj_type, schema, name, owner) for every non-comment TOC line."""
    for line in toc.splitlines():
        if line.startswith(";") or not line.strip():
            continue
        parts = line.split()
        entry = _split_toc_line(parts)
        if entry:
            yield entry


def _parse_schemas_from_toc(toc: str) -> list[str]:
    schemas: set[str] = set()
    for obj_type, schema, name, _ in _iter_toc(toc):
        if obj_type == "SCHEMA":
            candidate = name
        else:
            candidate = schema
        if candidate != "-" and not _SYSTEM_SCHEMA_RE.match(candidate):
            schemas.add(candidate)
    return sorted(schemas)


def _parse_owners_from_toc(toc: str) -> list[str]:
    """
    Extract all roles referenced in a pg_restore TOC dump.

    Only considers:
      - explicit ROLE/USER/GROUP objects
      - owners of objects
    Ignore system roles, table names, indexes, and schemas.
    """
    roles = set()
    for obj_type, schema, name, owner in _iter_toc(toc):
        if owner != "-" and not _SYSTEM_ROLE_RE.match(owner):
            roles.add(owner)
        if obj_type.upper() in ("ROLE", "USER", "GROUP"):
            if name != "-" and not _SYSTEM_ROLE_RE.match(name):
                roles.add(name)
    return sorted(roles)


def _parse_tables_from_toc(toc: str, schemas: list[str]) -> list[str]:
    schema_set = set(schemas)
    tables: list[str] = []
    for obj_type, schema, name, _ in _iter_toc(toc):
        if obj_type == "TABLE" and schema in schema_set:
            tables.append(f"{schema}.{name}")
    return tables


# ──────────────────────────────────────────────────────────────────────────────
#  Step implementations
# ──────────────────────────────────────────────────────────────────────────────

def _step_connection(cfg: Config, adapter: DbAdapter) -> None:
    _section("Step 1 — Connection")

    console.print(f"  Engine : [cyan]{cfg.db_type.value}[/]")
    if cfg.use_docker:
        console.print(f"  Mode   : [cyan]Docker[/] (service: {cfg.service})")
    else:
        console.print(f"  Mode   : Direct — {cfg.host}:{cfg.port}")
    console.print(f"  User   : {cfg.user}")
    console.print(f"  Source : {cfg.backup_dir}")
    console.print()

    if questionary.confirm("Override connection settings?", default=False).ask():
        if not cfg.use_docker:
            cfg.host = questionary.text("Host", default=cfg.host).ask() or cfg.host
            cfg.port = int(questionary.text("Port", default=str(cfg.port)).ask() or cfg.port)
        cfg.user = questionary.text("Username", default=cfg.user).ask() or cfg.user
        new_pass = questionary.password("Password (blank to keep current)").ask() or ""
        if new_pass:
            cfg.password = new_pass

    try:
        questionary.check_connection_with_countdown(adapter.check_connection, cfg.connection_timeout)
    except TimeoutError as exc:
        _die(str(exc))
    console.print("[green]Connection OK.[/]")


def _step_select_db_dir(cfg: Config) -> tuple[Path, str]:
    """Step 2: pick a database folder from BASE_DIR."""
    _section("Step 2 — Select Database")

    base = Path(cfg.backup_dir)
    db_dirs = sorted([d for d in base.iterdir() if d.is_dir()])
    if not db_dirs:
        _die(f"No database folders found in {base}")

    table = Table(show_header=True, header_style="bold")
    table.add_column("#", style="dim", width=4)
    table.add_column("Database", min_width=24)
    table.add_column("Size", justify="right")
    table.add_column("Backups", justify="right")
    for i, d in enumerate(db_dirs):
        size = subprocess.run(
            ["du", "-sh", str(d)], capture_output=True
        ).stdout.decode().split("\t")[0] if shutil.which("du") else "?"
        count = sum(len(list(d.glob(pat))) for pat in _BACKUP_EXTENSIONS)
        table.add_row(str(i), d.name, size, str(count))
    console.print(table)

    choices = [questionary.Choice(title=d.name, value=d) for d in db_dirs]
    db_dir: Path = questionary.select("Select database", choices=choices).ask()
    database = db_dir.name
    console.print(f"  Selected: [bold]{database}[/]")
    return db_dir, database


def _step_select_file(db_dir: Path, database: str) -> Path:
    """Step 3: pick a backup file from the database folder."""
    _section("Step 3 — Select Backup File")

    backups: list[Path] = []
    for pat in _BACKUP_EXTENSIONS:
        backups.extend(db_dir.glob(pat))
    backups = sorted(set(backups))

    if not backups:
        _die(f"No backup files found for '{database}' in {db_dir}")

    table = Table(show_header=True, header_style="bold")
    table.add_column("#", style="dim", width=4)
    table.add_column("File", min_width=40)
    table.add_column("Size", justify="right")
    for i, f in enumerate(backups):
        table.add_row(str(i), f.name, _human_size(f))
    console.print(table)

    choices = [questionary.Choice(title=f.name, value=f) for f in backups]
    chosen: Path = questionary.select(
        "Select backup file (latest = last entry)", choices=choices, default=backups[-1]
    ).ask()
    console.print(f"  Selected: [bold]{chosen.name}[/]")
    return chosen


def _step_schema_selection(toc: str) -> tuple[list[str], list[str]]:
    """Step 4 (PG only): parse schemas from TOC, let user pick."""
    _section("Step 4 — Schema Selection")

    schemas = _parse_schemas_from_toc(toc)

    if not schemas:
        console.print("  No named user schemas detected — restoring everything.")
        return [], []

    choices = [questionary.Choice(title=s, value=s) for s in schemas]
    selected = questionary.checkbox(
        "Select schemas to restore (blank = all)", choices=choices
    ).ask() or []

    schema_args: list[str] = []
    if selected:
        for s in selected:
            schema_args += ["-n", s]
        console.print(f"  Schema filter: [bold]{', '.join(selected)}[/]")
    else:
        console.print("  All schemas will be restored.")
        selected = schemas

    return schema_args, selected


def _step_table_selection(toc: str, selected_schemas: list[str]) -> list[str]:
    """Step 4b (PG only): optional table selection within chosen schemas."""
    if not selected_schemas:
        return []

    tables = _parse_tables_from_toc(toc, selected_schemas)
    if not tables:
        return []

    console.print()
    console.print(f"  [bold]Tables in selected schemas ({len(tables)} total):[/]")

    choices = [questionary.Choice(title=t, value=t) for t in tables]
    selected = questionary.checkbox(
        "Select tables (blank = all)", choices=choices
    ).ask() or []

    table_args: list[str] = []
    if selected:
        for entry in selected:
            table_args += ["-t", entry]
        console.print(f"  Table filter applied: {', '.join(selected)}")
    return table_args


def _step_schema_remap(selected_schemas: list[str]) -> dict[str, str]:
    """
    Optional step: let the user rename each selected schema on the target.

    Returns a mapping of {original_name: target_name} for schemas that should
    be renamed after pg_restore completes.  Schemas kept under their original
    name are omitted from the dict.

    Why post-restore rename: pg_restore has no --target-schema flag, so objects
    are always restored under the name embedded in the dump.  We rename
    afterwards using ALTER SCHEMA RENAME (fast, atomic) if the target name is
    free, or SET SCHEMA on each object if the target already exists (e.g.
    remapping 'fuelrod' into 'public').
    """
    _section("Step 4b — Schema Remapping (optional)")
    console.print(
        "  Dump schema(s): "
        + ", ".join(f"[bold]{s}[/]" for s in selected_schemas)
    )
    console.print("  Leave blank to keep each name as-is.\n")
    mapping: dict[str, str] = {}
    for schema in selected_schemas:
        new_name = (
            questionary.text(f"  Restore '{schema}' as", default=schema).ask() or schema
        ).strip()
        if new_name and new_name != schema:
            mapping[schema] = new_name

    if mapping:
        for src, dst in mapping.items():
            console.print(f"  [cyan]{src}[/] → [bold]{dst}[/]")
    else:
        console.print("  No remapping — schemas will be restored under their original names.")
    return mapping


def _step_role_analysis(toc: str, adapter) -> list[str]:
    """Step 5 (PG only): find missing roles, offer create / no-owner / ignore."""
    _section("Step 5 — Role Analysis")

    owners = _parse_owners_from_toc(toc)
    extra_args: list[str] = []

    if not owners:
        console.print("  No role information found in dump TOC.")
        return extra_args

    missing: list[str] = []
    for owner in owners:
        exists = adapter.role_exists(owner)
        marker = "[green]EXISTS [/]" if exists else "[red]MISSING[/]"
        console.print(f"  [{marker}]  {owner}")
        if not exists:
            missing.append(owner)

    if not missing:
        console.print("\n  [green]All roles present.[/]")
        return extra_args

    console.print()
    console.print(f"  [yellow]WARN:[/] {len(missing)} role(s) are missing on the target server.")
    console.print()

    action = questionary.select(
        "How should missing roles be handled?",
        choices=[
            questionary.Choice("Ignore (restore will warn/fail on ownership)", value="ignore"),
            questionary.Choice("Create missing roles interactively", value="create"),
            questionary.Choice("Restore with --no-owner --no-privileges (skip ownership)", value="no_owner"),
        ],
        default="ignore",
    ).ask()

    if action == "create":
        for role in missing:
            console.print(f"\n  Creating role: [bold]{role}[/]")
            superuser = questionary.confirm("  Superuser?", default=False).ask()
            can_login = questionary.confirm("  Can login?", default=True).ask()
            password = questionary.password("  Password (blank = no password)").ask() or None
            adapter.create_role(role, superuser=superuser, can_login=can_login, password=password)
            console.print(f"  [green]Role '{role}' created.[/]")
    elif action == "no_owner":
        extra_args += ["--no-owner", "--no-privileges"]
        console.print("  Will use --no-owner --no-privileges.")
    else:
        console.print("  [yellow]Ignoring missing roles — errors may appear in restore output.[/]")

    return extra_args


def _step_restore_options_pg() -> tuple[list[str], list[str], int, bool]:
    """Step 6 (PG only): scope, clean mode, parallelism, dry-run."""
    _section("Step 6 — Restore Options")

    scope_choice = questionary.select(
        "Restore scope",
        choices=[
            questionary.Choice("Full restore — schema + data", value="full"),
            questionary.Choice("Schema only", value="schema"),
            questionary.Choice("Data only", value="data"),
        ],
    ).ask()

    scope_args: list[str] = []
    if scope_choice == "schema":
        scope_args = ["--schema-only"]
    elif scope_choice == "data":
        scope_args = ["--data-only"]

    clean_args: list[str] = []
    if scope_choice != "data":
        clean_choice = questionary.select(
            "Object handling",
            choices=[
                questionary.Choice("Clean — DROP existing then recreate", value="clean"),
                questionary.Choice("Append — overlay onto existing objects", value="append"),
            ],
        ).ask()
        if clean_choice == "clean":
            clean_args = ["--clean", "--if-exists"]

    jobs_str = questionary.text("Parallel restore workers", default="1").ask() or "1"
    try:
        jobs = max(1, int(jobs_str))
    except ValueError:
        jobs = 1

    dry_run = questionary.confirm("Dry run? (show plan only — no changes made)", default=False).ask()

    return scope_args, clean_args, jobs, dry_run


def _step_target_db(database: str, dry_run: bool, adapter: DbAdapter) -> str:
    """Step 7: confirm target database, drop/recreate if needed."""
    _section("Step 7 — Target Database")

    target = questionary.text(
        "Restore into database name", default=database
    ).ask() or database

    if not dry_run:
        if adapter.db_exists(target):
            console.print(f"  [yellow]Database '{target}' already exists.[/]")
            drop_it = questionary.select(
                "Action",
                choices=[
                    questionary.Choice("Drop and recreate (clean slate)", value="drop"),
                    questionary.Choice("Keep existing (overlay)", value="keep"),
                ],
                default="keep",
            ).ask()
            if drop_it == "drop":
                killed = adapter.terminate_connections(target)
                if killed:
                    console.print(f"  [yellow]Terminated {killed} active connection(s) to '{target}'.[/]")
                console.print(f"  Dropping '{target}'...")
                adapter.drop_db(target)
                console.print(f"  Creating '{target}'...")
                adapter.create_db(target)
        else:
            console.print(f"  Creating '{target}'...")
            adapter.create_db(target)

    return target


# ──────────────────────────────────────────────────────────────────────────────
#  Execute restore (PostgreSQL-specific streaming)
# ──────────────────────────────────────────────────────────────────────────────

def _execute_pg_restore(
        backup_file: Path,
        target_db: str,
        restore_args: list[str],
        cfg: Config,
) -> None:
    """Stream the dump file into pg_restore."""
    base_args = [
                    "-U", cfg.user,
                    "-h", cfg.host,
                    "-p", str(cfg.port),
                    "-d", target_db,
                    "-v",
                ] + restore_args

    if cfg.use_docker:
        cmd = (
                ["docker", "exec", "-i",
                 "-e", f"PGPASSWORD={cfg.password}",
                 "-e", f"PGUSER={cfg.user}",
                 cfg.service,
                 cfg.pg_restore_cmd]
                + base_args
        )
        # Minimal env — password already injected via -e; don't leak host secrets.
        env = {"PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin")}
    else:
        cmd = [cfg.pg_restore_cmd] + base_args
        env = os.environ.copy()
        env["PGPASSWORD"] = cfg.password

    if backup_file.suffix == ".gz":
        with tempfile.NamedTemporaryFile(suffix=".dump", delete=False) as tmp_file:
            tmp = Path(tmp_file.name)
        try:
            with gzip.open(backup_file, "rb") as gz_in, tmp.open("wb") as f_out:
                shutil.copyfileobj(gz_in, f_out)
            with tmp.open("rb") as f_in:
                subprocess.run(cmd, stdin=f_in, env=env, check=True)
        finally:
            if tmp.exists():
                tmp.unlink()
    else:
        with backup_file.open("rb") as f_in:
            subprocess.run(cmd, stdin=f_in, env=env, check=True)


# ──────────────────────────────────────────────────────────────────────────────
#  Public entry point
# ──────────────────────────────────────────────────────────────────────────────

def run_restore(cfg: Config) -> None:
    """Main restore workflow (always interactive)."""
    adapter = get_adapter(cfg)

    if not cfg.password:
        _die("Password is required. Set the appropriate *_PASSWORD variable in .backup.")
    if not cfg.backup_dir or not Path(cfg.backup_dir).is_dir():
        _die(f"Backup directory not found: {cfg.backup_dir}")

    console.print(Panel(f"[bold cyan]{cfg.db_type.value.upper()} Restore Wizard[/]", expand=False))

    # Step 1 — Connection
    _step_connection(cfg, adapter)

    # Step 2 — Select database folder
    db_dir, database = _step_select_db_dir(cfg)

    # Step 3 — Select backup file
    backup_file = _step_select_file(db_dir, database)

    # ── PostgreSQL-specific: TOC, schema, role, scope analysis ─────
    toc = ""
    schema_args: list[str] = []
    table_args: list[str] = []
    role_args: list[str] = []
    schema_remap: dict[str, str] = {}
    scope_args: list[str] = []
    clean_args: list[str] = []
    jobs = 1
    dry_run = False
    selected_schemas: list[str] = []

    if adapter.supports_toc:
        _section("Analysing Dump")
        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}", table_column=Column(min_width=44)),
            TimeElapsedColumn(),
            console=console,
        ) as _p:
            _t = _p.add_task("[cyan]Reading table of contents…[/]", total=None)
            try:
                toc = adapter.read_toc(backup_file)
                _p.update(_t, description="[green]✓ TOC loaded[/]", total=1, completed=1)
            except subprocess.CalledProcessError as exc:
                _p.update(_t, description="[red]✗ TOC read failed[/]", total=1, completed=1)
                _die(f"Failed to read dump TOC: {exc}")

        meta_lines = [
            line.lstrip("; ") for line in toc.splitlines()
            if line.startswith(";") and any(
                kw in line for kw in ("dbname", "Dump Version", "Dumped from", "Dumped by", "Format", "Compression")
            )
        ]
        if meta_lines:
            console.print("\n  [bold]Dump metadata:[/]")
            for ml in meta_lines:
                console.print(f"    {ml}")

        if adapter.supports_schemas:
            schema_args, selected_schemas = _step_schema_selection(toc)
            table_args = _step_table_selection(toc, selected_schemas)
            if selected_schemas:
                schema_remap = _step_schema_remap(selected_schemas)

        if adapter.supports_roles:
            role_args = _step_role_analysis(toc, adapter)

        scope_args, clean_args, jobs, dry_run = _step_restore_options_pg()

    elif adapter.supports_schemas:
        # Non-PG engine with schemas (e.g. MSSQL)
        available = adapter.get_user_schemas(database)
        if available:
            _section("Step 4 — Schema Selection")
            choices = [questionary.Choice(title=s, value=s) for s in available]
            selected_schemas = questionary.checkbox(
                "Select schemas to restore (blank = all)", choices=choices
            ).ask() or []

        dry_run = questionary.confirm("Dry run? (show plan only — no changes made)", default=False).ask()
    else:
        # MariaDB / plain SQL
        dry_run = questionary.confirm("Dry run? (show plan only — no changes made)", default=False).ask()

    # Step 7 — Target database
    target_db = _step_target_db(database, dry_run, adapter)

    # ── Summary ────────────────────────────────────────────────────
    console.print()
    console.print(Panel("[bold]RESTORE SUMMARY[/]", expand=False))
    console.print(f"  Engine      : [cyan]{cfg.db_type.value}[/]")
    console.print(f"  Source file : [bold]{backup_file.name}[/]")
    console.print(f"  Target DB   : [bold]{target_db}[/]")
    if adapter.supports_schemas:
        console.print(f"  Schemas     : {', '.join(selected_schemas) or 'all'}")
        if schema_remap:
            for src, dst in schema_remap.items():
                console.print(f"  Remap       : [cyan]{src}[/] → [bold]{dst}[/]")
    if adapter.supports_toc:
        console.print(f"  Scope       : {scope_args[0].lstrip('-') if scope_args else 'full'}")
        console.print(f"  Drop first  : {'yes' if clean_args else 'no'}")
        console.print(f"  Workers     : {jobs}")
        console.print(f"  No-owner    : {'yes' if '--no-owner' in role_args else 'no'}")
    console.print(f"  Dry run     : {dry_run}")
    console.print()

    if dry_run:
        console.print("[yellow]Dry run complete. No changes were made.[/]")
        return

    if not questionary.confirm("Proceed with restore? This may be destructive.", default=True).ask():
        console.print("[yellow]Aborted by user.[/]")
        sys.exit(0)

    # ── Execute ────────────────────────────────────────────────────
    console.print()
    console.print(f"  Restoring [bold]{backup_file.name}[/] → [bold]{target_db}[/]")

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}", table_column=Column(min_width=44)),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("[cyan]Preparing…[/]", total=None)

        try:
            if adapter.supports_toc:
                restore_args: list[str] = []
                restore_args += clean_args
                restore_args += scope_args
                restore_args += schema_args
                restore_args += table_args
                restore_args += role_args
                if jobs > 1:
                    restore_args += ["-j", str(jobs)]

                schemas_to_ensure = selected_schemas or _parse_schemas_from_toc(toc)
                if schemas_to_ensure:
                    progress.update(task, description=f"[cyan]Ensuring {len(schemas_to_ensure)} schema(s)…[/]")
                    adapter.ensure_schemas(target_db, schemas_to_ensure)

                progress.update(task, description=f"[cyan]Running pg_restore ({backup_file.name})…[/]")
                _execute_pg_restore(backup_file, target_db, restore_args, cfg)
            else:
                progress.update(task, description=f"[cyan]Restoring {backup_file.name}…[/]")
                no_owner = "--no-owner" in role_args
                adapter.restore_db(
                    target_db,
                    backup_file,
                    schemas=selected_schemas,
                    no_owner=no_owner,
                )

            if adapter.supports_toc and schema_remap:
                for src, dst in schema_remap.items():
                    progress.update(task, description=f"[cyan]Remapping {src} → {dst}…[/]")
                    try:
                        adapter.remap_schema(src, dst, target_db)
                    except Exception as exc:  # noqa: BLE001
                        console.print(f"  [yellow]WARN:[/] remap {src} → {dst} failed: {exc}")

            progress.update(task, description=f"[green]✓ Restore complete → {target_db}[/]", total=1, completed=1)

        except subprocess.CalledProcessError as exc:
            progress.update(task, description="[red]✗ Restore failed[/]", total=1, completed=1)
            _die(f"Restore failed (exit {exc.returncode}). Check output above for details.")
        except Exception as exc:
            progress.update(task, description="[red]✗ Restore failed[/]", total=1, completed=1)
            _die(f"Restore failed: {exc}")

    # ── Post-restore stats (PG only) ───────────────────────────────
    if adapter.supports_toc:
        _section("Post-Restore Report")
        table_count = adapter.get_table_count(target_db)
        console.print(f"  Tables restored : {table_count}")
        for schema in selected_schemas:
            effective = schema_remap.get(schema, schema)
            cnt = adapter.get_table_count(target_db, schema=effective)
            label = f"{schema} → {effective}" if schema in schema_remap else schema
            console.print(f"    {label:<36} {cnt} tables")

    console.print()
    console.print(Panel(f"[bold green]RESTORE COMPLETE → {target_db}[/]", expand=False))
