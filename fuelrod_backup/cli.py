"""Typer CLI entry point: `fuelrod-backup backup` and `fuelrod-backup restore`."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel

from .config import DbType, load_all_configs, load_config

app = typer.Typer(
    name="fuelrod-backup",
    help="Interactive database backup and restore tool (PostgreSQL, MariaDB, MSSQL).",
    add_completion=False,
)
console = Console()

# Reusable option definitions
_CONFIG_OPT = typer.Option("--config", "-c", help="Path to .backup or .env config file.", exists=True, dir_okay=False)
_DOCKER_OPT = typer.Option("--docker/--no-docker", help="Override USE_DOCKER from config (highest priority).")
_DB_TYPE_OPT = typer.Option("--db-type", "-t", help="Database engine: postgres | mariadb | mssql.")


def _apply_docker_override(cfg, use_docker: bool | None) -> None:
    """Apply --docker/--no-docker CLI flag if explicitly provided."""
    if use_docker is not None:
        cfg.use_docker = use_docker


def _validate_db_type(db_type: str | None) -> None:
    if db_type is not None and db_type.lower() not in (e.value for e in DbType):
        console.print(f"[bold red]ERROR:[/] Unknown --db-type '{db_type}'. Choose: postgres, mariadb, mssql")
        raise typer.Exit(code=1)


@app.command()
def backup(
        no_interactive: Annotated[
            bool,
            typer.Option("--no-interactive", "-n", help="Skip all wizard prompts; back up all databases."),
        ] = False,
        all_engines: Annotated[
            bool,
            typer.Option(
                "--all-engines", "-a",
                help="Backup every engine configured with PG_/MY_/MS_ prefixes in parallel (non-interactive).",
            ),
        ] = False,
        compress: Annotated[
            bool | None,
            typer.Option("--compress/--no-compress", help="Compress output with gzip."),
        ] = None,
        keep_days: Annotated[
            int | None,
            typer.Option("--keep-days", "-k", help="Delete backups older than N days (0 = keep forever)."),
        ] = None,
        databases: Annotated[
            list[str],
            typer.Option("--db", "-d", help="Database(s) to back up (repeatable). Default: all."),
        ] = [],
        use_docker: Annotated[bool | None, _DOCKER_OPT] = None,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Back up one or more databases (postgres | mariadb | mssql).

    Use --all-engines to back up every engine whose PG_/MY_/MS_ credentials
    are configured, running all engines in parallel.
    """
    from .backup import run_backup, run_parallel_backup

    _validate_db_type(db_type)
    if all_engines:
        configs = load_all_configs(config_file)
        for cfg in configs:
            _apply_docker_override(cfg, use_docker)
        run_parallel_backup(
            configs,
            databases=list(databases) or None,
            compress=compress,
            keep_days=keep_days,
        )
        return

    cfg = load_config(config_file, db_type_override=db_type)
    _apply_docker_override(cfg, use_docker)
    run_backup(
        cfg,
        interactive=not no_interactive,
        databases=list(databases) or None,
        compress=compress,
        keep_days=keep_days,
    )


@app.command()
def restore(
        all_engines: Annotated[
            bool,
            typer.Option(
                "--all-engines", "-a",
                help="Run the restore wizard for each engine configured with PG_/MY_/MS_ prefixes, one after another.",
            ),
        ] = False,
        use_docker: Annotated[bool | None, _DOCKER_OPT] = None,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Interactively restore a database from a dump file (postgres | mariadb | mssql).

    Use --all-engines to step through the restore wizard for every engine
    whose PG_/MY_/MS_ credentials are configured, one engine at a time.
    """
    from .restore import run_restore

    _validate_db_type(db_type)
    if all_engines:
        configs = load_all_configs(config_file)
        for cfg in configs:
            _apply_docker_override(cfg, use_docker)
        if len(configs) > 1:
            console.print(
                f"[bold cyan]Restoring {len(configs)} engine(s):[/] "
                + ", ".join(c.db_type.value for c in configs)
            )
        for cfg in configs:
            if len(configs) > 1:
                console.print(f"\n[bold cyan]─── {cfg.db_type.value.upper()} ───[/]")
            run_restore(cfg)
        return

    cfg = load_config(config_file, db_type_override=db_type)
    _apply_docker_override(cfg, use_docker)
    run_restore(cfg)


@app.command("drop")
def drop(
        use_docker: Annotated[bool | None, _DOCKER_OPT] = None,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Interactively drop a database or schema.

    Kills all active connections before dropping a database.
    Schema drop uses CASCADE (removes all objects inside the schema).
    Requires typing the name to confirm — operation is irreversible.
    """
    from .drop import run_drop

    _validate_db_type(db_type)
    cfg = load_config(config_file, db_type_override=db_type)
    _apply_docker_override(cfg, use_docker)
    run_drop(cfg)


@app.command("test")
def test_connection(
        use_docker: Annotated[bool | None, _DOCKER_OPT] = None,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Test the database connection and print resolved settings."""
    from .adapters import get_adapter

    _validate_db_type(db_type)
    cfg = load_config(config_file, db_type_override=db_type)
    _apply_docker_override(cfg, use_docker)

    pass_hint = f"{'*' * min(len(cfg.password), 6)}  ({len(cfg.password)} chars)" if cfg.password else "[red]NOT SET[/]"
    source = str(cfg.config_source) if cfg.config_source else "[red]none found — using defaults only[/]"
    docker_override = " [yellow](CLI override)[/]" if use_docker is not None else ""

    console.print()
    console.print("[bold]Resolved settings:[/]")
    console.print(f"  Config source : {source}")
    console.print(f"  DB type       : [cyan]{cfg.db_type.value}[/]")
    mode = f"[cyan]Docker[/] — service {cfg.service!r}" if cfg.use_docker else "Direct"
    console.print(f"  Mode          : {mode}{docker_override}")
    console.print(f"  Host          : {cfg.host}:{cfg.port}")
    console.print(f"  User          : {cfg.user}")
    console.print(f"  Password      : {pass_hint}")
    console.print(f"  Backup dir    : {cfg.backup_dir}")
    console.print(f"  Compress      : {cfg.compress}")
    console.print(f"  Retain        : {cfg.days_to_keep} days")
    console.print()

    adapter = get_adapter(cfg)
    try:
        adapter.check_connection()
        console.print("[bold green]✓ Connection successful.[/]")
    except Exception as exc:
        console.print(f"[bold red]✗ Connection failed:[/] {exc}")
        raise typer.Exit(code=1)


@app.command("init")
def init_config(
        output: Annotated[
            Path,
            typer.Option("--output", "-o", help="Path for the config file.", dir_okay=False),
        ] = Path(".backup"),
) -> None:
    """Create or update a .backup config file interactively."""
    from . import prompt as q
    from .config import _find_config_file, _parse_env_file

    output = output.resolve()
    updating = output.exists()

    existing_source: Path | None = output if updating else _find_config_file()
    existing_cfg = load_config(existing_source) if existing_source else None
    existing_raw: dict[str, str] = (
        _parse_env_file(existing_source) if existing_source and existing_source.is_file() else {}
    )

    console.print()
    if updating:
        title = f"[bold cyan]fuelrod-backup — update config[/]\n[dim]{output}[/]"
    else:
        title = "[bold cyan]fuelrod-backup — init wizard[/]\nNo existing config found — creating a new one."
    console.print(Panel(title, expand=False))

    if existing_source and not updating:
        console.print(f"  [yellow]Note:[/] Pre-filling from auto-discovered config: [dim]{existing_source}[/]")

    console.print(f"\n  Config will be written to: [bold]{output}[/]\n")

    # ── Config mode ────────────────────────────────────────────────
    console.rule("[bold cyan]Config mode[/]")
    has_prefixed = any(
        k.startswith(("PG_", "MY_", "MS_")) for k in existing_raw
    )
    mode: str = q.select(
        "Configuration mode",
        choices=[
            q.Choice("Single-engine  (DB_TYPE + DB_* keys)", value="single"),
            q.Choice("Multi-engine   (PG_* / MY_* / MS_* keys, parallel backup)", value="multi"),
        ],
        default="multi" if has_prefixed else "single",
    ).ask()

    if mode == "multi":
        _init_multi_engine(q, output, updating, existing_raw)
    else:
        _init_single_engine(q, output, updating, existing_cfg, existing_raw)


# ──────────────────────────────────────────────────────────────────────────────
#  init helpers
# ──────────────────────────────────────────────────────────────────────────────

def _write_config(output: Path, lines: list[str], updating: bool) -> None:
    output.write_text("\n".join(lines), encoding="utf-8")
    try:
        output.chmod(0o600)
    except OSError:
        pass
    verb = "updated" if updating else "written"
    console.print(f"\n[bold green]✓[/] Config {verb}: [bold]{output}[/]")
    console.print("  [yellow]Note:[/] This file contains a plaintext password — keep it private.")
    console.print(f"\n  Verify with: [bold]fuelrod-backup test --config {output}[/]\n")


def _collect_engine_settings(q, engine: str, prefix: str, raw: dict[str, str]) -> dict[str, str]:
    """Interactively collect per-engine connection settings, returning a key→value dict."""
    if engine == "mariadb":
        def_user, def_port, def_service = "root", "3306", "mariadb"
    elif engine == "mssql":
        def_user, def_port, def_service = "sa", "1433", "mssql"
    else:
        def_user, def_port, def_service = "postgres", "5432", "postgres"

    def ex(short: str, default: str = "") -> str:
        return raw.get(prefix + short, default)

    console.print()
    console.rule(f"[bold cyan]{engine.upper()} connection mode[/]")
    use_docker: bool = q.select(
        f"How does the tool connect to {engine}?",
        choices=[
            q.Choice("Docker  (exec into a running container)", value=True),
            q.Choice("Direct  (host:port, no Docker)", value=False),
        ],
        default=ex("USE_DOCKER", "true").lower() in ("true", "1", "yes"),
    ).ask()

    if use_docker:
        service = q.text(f"Container name ({prefix}SERVICE)", default=ex("SERVICE", def_service)).ask() or def_service
        host = ex("HOST", "127.0.0.1")
        port = ex("PORT", def_port)
    else:
        service = ex("SERVICE", def_service)
        host = q.text(f"Host ({prefix}HOST)", default=ex("HOST", "127.0.0.1")).ask() or "127.0.0.1"
        port = q.text(f"Port ({prefix}PORT)", default=ex("PORT", def_port)).ask() or def_port

    console.print()
    console.rule(f"[bold cyan]{engine.upper()} credentials[/]")
    username = q.text(f"Username ({prefix}USERNAME)", default=ex("USERNAME", def_user)).ask() or def_user
    ex_pass = ex("PASSWORD", "")
    if ex_pass:
        change = q.confirm("Change password? (current is set)", default=False).ask()
        password = q.password(f"New password ({prefix}PASSWORD)").ask() or ex_pass if change else ex_pass
    else:
        password = q.password(f"Password ({prefix}PASSWORD)").ask() or ""

    console.print()
    console.rule(f"[bold cyan]{engine.upper()} advanced[/]")
    settings: dict[str, str] = {
        "USERNAME": username,
        "PASSWORD": password,
        "HOST": host,
        "PORT": port,
        "SERVICE": service,
        "USE_DOCKER": "true" if use_docker else "false",
    }

    if engine == "postgres":
        settings["DUMP_CMD"] = (
            q.text(f"pg_dump command ({prefix}DUMP_CMD)", default=ex("DUMP_CMD", "pg_dump")).ask()
            or "pg_dump"
        )
        settings["RESTORE_CMD"] = (
            q.text(f"pg_restore command ({prefix}RESTORE_CMD)", default=ex("RESTORE_CMD", "pg_restore")).ask()
            or "pg_restore"
        )
        settings["CMD"] = (
            q.text(f"psql command ({prefix}CMD)", default=ex("CMD", "psql")).ask()
            or "psql"
        )
    elif engine == "mariadb":
        settings["DUMP_CMD"] = (
            q.text(f"Dump command ({prefix}DUMP_CMD)", default=ex("DUMP_CMD", "mariadb-dump")).ask()
            or "mariadb-dump"
        )
        settings["CMD"] = (
            q.text(f"Client command ({prefix}CMD)", default=ex("CMD", "mysql")).ask()
            or "mysql"
        )
    elif engine == "mssql":
        settings["BACKUP_DIR"] = q.text(
            f"Backup dir inside container ({prefix}BACKUP_DIR)",
            default=ex("BACKUP_DIR", "/var/opt/mssql/backups"),
        ).ask() or "/var/opt/mssql/backups"

    return settings


def _init_multi_engine(q, output: Path, updating: bool, existing_raw: dict[str, str]) -> None:
    from .config import _ENGINE_PREFIXES

    engine_labels = {
        "postgres": "PostgreSQL",
        "mariadb":  "MariaDB / MySQL",
        "mssql":    "Microsoft SQL Server",
    }
    prefixes = dict(_ENGINE_PREFIXES)  # {engine: prefix}

    # Pre-select engines that already have prefixed keys in the file
    active = {e for e, p in prefixes.items() if any(k.startswith(p) for k in existing_raw)}

    console.print()
    console.rule("[bold cyan]Engines to configure[/]")
    default_active = active or {"postgres"}
    selected_engines: list[str] = q.checkbox(
        "Select engines (Space to toggle, Enter to confirm)",
        choices=[
            q.Choice(title=engine_labels[e], value=e, checked=(e in default_active))
            for e in prefixes
        ],
    ).ask() or []

    if not selected_engines:
        console.print("[yellow]No engines selected — aborted.[/]")
        raise typer.Exit(0)

    # Collect per-engine settings
    engine_settings: dict[str, dict[str, str]] = {}
    for engine in selected_engines:
        prefix = prefixes[engine]
        console.print(f"\n[bold cyan]━━━ {engine_labels[engine]} ({prefix}*) ━━━[/]")
        engine_settings[engine] = _collect_engine_settings(q, engine, prefix, existing_raw)

    # Shared settings
    console.print()
    console.rule("[bold cyan]Shared settings[/]")
    ex_base    = existing_raw.get("BASE_DIR",          str(output.parent / "db-backup"))
    ex_compress = existing_raw.get("COMPRESS_FILE",    "true")
    ex_keep    = existing_raw.get("KEEP_DAYS",         "7")
    ex_timeout = existing_raw.get("CONNECTION_TIMEOUT","30")

    base_dir = q.text(
        "Backup root directory (BASE_DIR)  [dim]/<engine> appended automatically[/]",
        default=ex_base,
    ).ask() or ex_base
    compress: bool = q.confirm(
        "Compress backups with gzip? (COMPRESS_FILE)",
        default=ex_compress.lower() in ("true", "1", "yes"),
    ).ask()
    keep_days_str = q.text("Retain backups for N days — 0 = keep forever (KEEP_DAYS)", default=ex_keep).ask() or ex_keep
    try:
        keep_days = max(0, int(keep_days_str))
    except ValueError:
        keep_days = 7
    timeout_str = q.text("Connection timeout in seconds (CONNECTION_TIMEOUT)", default=ex_timeout).ask() or ex_timeout
    try:
        conn_timeout = max(1, int(timeout_str))
    except ValueError:
        conn_timeout = 30

    # Summary
    console.print()
    console.print(Panel("[bold]Config summary — multi-engine[/]", expand=False))
    console.print(f"  Output file : [bold]{output}[/]")
    console.print(f"  Engines     : [cyan]{', '.join(selected_engines)}[/]")
    console.print(f"  Backup dir  : {base_dir}/<engine>/")
    console.print(f"  Compress    : {compress}")
    console.print(f"  Retain      : {keep_days} days")
    console.print(f"  Timeout     : {conn_timeout}s")
    for engine, settings in engine_settings.items():
        mode_str = (
            f"Docker — {settings['SERVICE']}"
            if settings["USE_DOCKER"] == "true"
            else f"Direct — {settings['HOST']}:{settings['PORT']}"
        )
        pass_display = "(set)" if settings["PASSWORD"] else "[red]NOT SET[/]"
        console.print(f"  [{engine}] {mode_str}  user={settings['USERNAME']}  pass={pass_display}")
    console.print()

    action = "Update" if updating else "Write"
    if not q.confirm(f"{action} config file?", default=True).ask():
        console.print("[yellow]Aborted.[/]")
        raise typer.Exit(0)

    # Build file
    engine_comment = {
        "postgres": "PostgreSQL (PG_*)",
        "mariadb":  "MariaDB / MySQL (MY_*)",
        "mssql":    "MSSQL (MS_*)",
    }
    lines: list[str] = [
        "# fuelrod-backup configuration — multi-engine mode",
        f"# {'Updated' if updating else 'Generated'} by: fuelrod-backup init",
        "#",
        "# Run: fuelrod-backup backup --all-engines",
        "",
        "# ── Shared ──────────────────────────────────────────────────────",
        f"BASE_DIR={base_dir}",
        f"COMPRESS_FILE={'true' if compress else 'false'}",
        f"KEEP_DAYS={keep_days}",
        f"CONNECTION_TIMEOUT={conn_timeout}",
        "",
    ]

    for engine in selected_engines:
        prefix = prefixes[engine]
        settings = engine_settings[engine]
        lines += [f"# ── {engine_comment[engine]} ──────────────────────────────────────"]
        for short, val in settings.items():
            lines.append(f"{prefix}{short}={val}")
        lines.append("")

    _write_config(output, lines, updating)


def _init_single_engine(q, output: Path, updating: bool, existing_cfg, existing_raw: dict[str, str]) -> None:
    # ── Engine ─────────────────────────────────────────────────────
    console.rule("[bold cyan]Database engine[/]")
    existing_db_type = existing_cfg.db_type.value if existing_cfg else "postgres"
    db_type: str = q.select(
        "Database engine",
        choices=[
            q.Choice("PostgreSQL", value="postgres"),
            q.Choice("MariaDB / MySQL", value="mariadb"),
            q.Choice("Microsoft SQL Server", value="mssql"),
        ],
        default=existing_db_type,
    ).ask()

    if db_type == "mariadb":
        _def_user, _def_port, _def_service = "root", "3306", "mariadb"
        _def_dump_cmd, _def_client_cmd = "mariadb-dump", "mysql"
    elif db_type == "mssql":
        _def_user, _def_port, _def_service = "sa", "1433", "mssql"
        _def_dump_cmd, _def_client_cmd = "", ""
    else:
        _def_user, _def_port, _def_service = "postgres", "5432", "postgres"
        _def_dump_cmd, _def_client_cmd = "pg_dump", "psql"

    ex = existing_cfg
    ex_use_docker = ex.use_docker if ex else True
    ex_service    = ex.service    if ex else _def_service
    ex_host       = ex.host       if ex else "127.0.0.1"
    ex_port       = str(ex.port)  if ex else _def_port
    ex_user       = ex.user       if ex else _def_user
    ex_pass       = ex.password   if ex else ""
    ex_base_dir   = ex.base_dir   if ex else str(output.parent / "db-backup")
    ex_compress   = ex.compress   if ex else True
    ex_keep_days  = ex.days_to_keep if ex else 7
    ex_timeout    = ex.connection_timeout if ex else 30
    ex_pg_dump    = ex.pg_dump_cmd    if ex else "pg_dump"
    ex_pg_restore = ex.pg_restore_cmd if ex else "pg_restore"
    ex_mysql_dump = ex.mysql_dump_cmd if ex else _def_dump_cmd if db_type == "mariadb" else "mariadb-dump"
    ex_mysql_cmd  = ex.mysql_cmd      if ex else _def_client_cmd if db_type == "mariadb" else "mysql"
    ex_mssql_dir  = ex.mssql_backup_dir if ex else "/var/opt/mssql/backups"

    # ── Connection mode ────────────────────────────────────────────
    console.print()
    console.rule("[bold cyan]Connection mode[/]")
    use_docker: bool = q.select(
        "How does the tool connect to the database?",
        choices=[
            q.Choice("Docker  (exec into a running container)", value=True),
            q.Choice("Direct  (host:port, no Docker)", value=False),
        ],
        default=ex_use_docker,
    ).ask()

    if use_docker:
        service = q.text("Container name (SERVICE)", default=ex_service).ask() or ex_service
        host, port = ex_host, ex_port
    else:
        service = ex_service
        host = q.text("Host (DB_HOST)", default=ex_host).ask() or ex_host
        port = q.text("Port (DB_PORT)", default=ex_port).ask() or ex_port

    # ── Credentials ────────────────────────────────────────────────
    console.print()
    console.rule("[bold cyan]Credentials[/]")
    username = q.text("Username (DB_USERNAME)", default=ex_user).ask() or ex_user
    if updating and ex_pass:
        change_pass = q.confirm("Change password? (current password is set)", default=False).ask()
        password = q.password("New password (DB_PASSWORD)").ask() or ex_pass if change_pass else ex_pass
    else:
        password = q.password("Password (DB_PASSWORD)").ask() or ex_pass

    # ── Backup storage ─────────────────────────────────────────────
    console.print()
    console.rule("[bold cyan]Backup storage[/]")
    base_dir = q.text(
        "Backup root directory (BASE_DIR)  [dim]/<db_type> appended automatically[/]",
        default=ex_base_dir,
    ).ask() or ex_base_dir

    compress: bool = q.confirm("Compress backups with gzip? (COMPRESS_FILE)", default=ex_compress).ask()

    keep_days_str = q.text(
        "Retain backups for N days — 0 = keep forever (KEEP_DAYS)",
        default=str(ex_keep_days),
    ).ask() or str(ex_keep_days)
    try:
        keep_days = max(0, int(keep_days_str))
    except ValueError:
        keep_days = ex_keep_days

    # ── Advanced ───────────────────────────────────────────────────
    console.print()
    console.rule("[bold cyan]Advanced[/]")
    timeout_str = q.text(
        "Connection timeout in seconds (CONNECTION_TIMEOUT)",
        default=str(ex_timeout),
    ).ask() or str(ex_timeout)
    try:
        conn_timeout = max(1, int(timeout_str))
    except ValueError:
        conn_timeout = ex_timeout

    if db_type == "postgres":
        pg_dump_cmd    = q.text("pg_dump command (PG_DUMP_CMD)",       default=ex_pg_dump).ask()    or ex_pg_dump
        pg_restore_cmd = q.text("pg_restore command (PG_RESTORE_CMD)", default=ex_pg_restore).ask() or ex_pg_restore
    elif db_type == "mariadb":
        mysql_dump_cmd = q.text("Dump command (MYSQL_DUMP_CMD)",   default=ex_mysql_dump).ask() or ex_mysql_dump
        mysql_cmd      = q.text("Client command (MYSQL_CMD)",       default=ex_mysql_cmd).ask()  or ex_mysql_cmd
    else:
        mssql_backup_dir = q.text(
            "Backup directory inside container (MSSQL_BACKUP_DIR)",
            default=ex_mssql_dir,
        ).ask() or ex_mssql_dir

    # ── Summary ────────────────────────────────────────────────────
    console.print()
    console.print(Panel("[bold]Config summary[/]", expand=False))
    console.print(f"  Output file   : [bold]{output}[/]")
    console.print(f"  Engine        : [cyan]{db_type}[/]")
    console.print(f"  Mode          : {'Docker — ' + service if use_docker else f'Direct — {host}:{port}'}")
    console.print(f"  User          : {username}")
    console.print(f"  Password      : {'(set)' if password else '[red]NOT SET[/]'}")
    console.print(f"  Backup dir    : {base_dir}/{db_type}/")
    console.print(f"  Compress      : {compress}")
    console.print(f"  Retain        : {keep_days} days")
    console.print(f"  Timeout       : {conn_timeout}s")
    console.print()

    action = "Update" if updating else "Write"
    if not q.confirm(f"{action} config file?", default=True).ask():
        console.print("[yellow]Aborted.[/]")
        raise typer.Exit(0)

    lines: list[str] = [
        "# fuelrod-backup configuration",
        f"# {'Updated' if updating else 'Generated'} by: fuelrod-backup init",
        "#",
        "# Place this file in the directory you run fuelrod-backup from,",
        "# or pass it explicitly with: fuelrod-backup --config /path/to/.backup",
        "",
        f"DB_TYPE={db_type}",
        "",
        "# ── Connection ──────────────────────────────────────────────────",
        f"DB_USERNAME={username}",
        f"DB_PASSWORD={password}",
        f"DB_HOST={host}",
        f"DB_PORT={port}",
        "",
        "# ── Docker ──────────────────────────────────────────────────────",
        f"USE_DOCKER={'true' if use_docker else 'false'}",
        f"SERVICE={service}",
        "",
        "# ── Backup storage ──────────────────────────────────────────────",
        f"BASE_DIR={base_dir}",
        f"COMPRESS_FILE={'true' if compress else 'false'}",
        f"KEEP_DAYS={keep_days}",
        "",
        "# ── Timeouts ────────────────────────────────────────────────────",
        f"CONNECTION_TIMEOUT={conn_timeout}",
        "",
    ]

    if db_type == "postgres":
        lines += [
            "# ── PostgreSQL binaries (on PATH or inside container) ───────────",
            f"PG_DUMP_CMD={pg_dump_cmd}",
            f"PG_RESTORE_CMD={pg_restore_cmd}",
            "",
        ]
    elif db_type == "mariadb":
        lines += [
            "# ── MariaDB / MySQL binaries ────────────────────────────────────",
            f"MYSQL_DUMP_CMD={mysql_dump_cmd}",
            f"MYSQL_CMD={mysql_cmd}",
            "",
        ]
    else:
        lines += [
            "# ── MSSQL ───────────────────────────────────────────────────────",
            f"MSSQL_BACKUP_DIR={mssql_backup_dir}",
            "",
        ]

    _write_config(output, lines, updating)


@app.command("n8n-backup")
def n8n_backup_cmd(
        no_interactive: Annotated[
            bool,
            typer.Option("--no-interactive", "-n", help="Skip wizard; back up all services."),
        ] = False,
        services: Annotated[
            list[str],
            typer.Option("--service", "-s", help="Service(s) to back up (repeatable). Default: all."),
        ] = [],
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Back up n8n Docker volumes (hot snapshot, no downtime)."""
    from .n8n_backup import run_n8n_backup

    cfg = load_config(config_file)
    run_n8n_backup(cfg, interactive=not no_interactive, services=list(services) or None)


@app.command("n8n-restore")
def n8n_restore_cmd(
        service: Annotated[
            str | None,
            typer.Option("--service", "-s", help="Service name to restore."),
        ] = None,
        backup_file: Annotated[
            Path | None,
            typer.Option("--file", "-f", help="Backup .tar.gz to restore directly.", exists=True, dir_okay=False),
        ] = None,
        dry_run: Annotated[
            bool,
            typer.Option("--dry-run", help="Show plan only — no changes made."),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option("--verbose", "-v", help="Print detailed step logs."),
        ] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Restore an n8n Docker volume from a backup archive."""
    from .n8n_restore import run_n8n_restore

    cfg = load_config(config_file)
    run_n8n_restore(cfg, service=service, backup_file=backup_file, dry_run=dry_run, verbose=verbose)


@app.command("gdrive-sync")
def gdrive_sync_cmd(
        dry_run: Annotated[
            bool,
            typer.Option("--dry-run", "-d", help="Show what would happen — no files moved or deleted."),
        ] = False,
        gdrive: Annotated[
            str | None,
            typer.Option("--gdrive", "-g", help="Google Drive remote folder name (overrides GDRIVE)."),
        ] = None,
        days: Annotated[
            int | None,
            typer.Option("--days", "-n", help="Prune remote files older than N days (overrides BACKUP_AGE)."),
        ] = None,
        include: Annotated[
            list[str],
            typer.Option("--include", "-i", help="Glob pattern to include (repeatable, overrides INCLUDE_FILES)."),
        ] = [],
        keep_local: Annotated[
            bool,
            typer.Option("--keep-local", help="Do NOT delete local files after a successful upload."),
        ] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Sync local backups to Google Drive via rclone, then prune old remote files."""
    from .gdrive_sync import run_gdrive_sync

    cfg = load_config(config_file)
    run_gdrive_sync(
        cfg,
        dry_run=dry_run,
        gdrive_remote=gdrive,
        age_days=days,
        include_patterns=list(include) or None,
        delete_local=not keep_local,
    )


@app.command("migrate")
def migrate(
        source_db: Annotated[
            str | None,
            typer.Option("--source-db", "-s", help="Source MariaDB database name."),
        ] = None,
        target_db: Annotated[
            str | None,
            typer.Option("--target-db", "-t", help="Target PostgreSQL database name (default: same as source)."),
        ] = None,
        target_schema: Annotated[
            str,
            typer.Option("--target-schema", help="PostgreSQL schema to migrate into."),
        ] = "public",
        batch_size: Annotated[
            int,
            typer.Option("--batch-size", "-b", help="Rows per INSERT batch."),
        ] = 1000,
        parallel: Annotated[
            int,
            typer.Option("--parallel", "-p", help="Number of tables to migrate in parallel."),
        ] = 4,
        dry_run: Annotated[
            bool,
            typer.Option("--dry-run", help="Show migration plan only — no writes."),
        ] = False,
        no_interactive: Annotated[
            bool,
            typer.Option("--no-interactive", "-n", help="Skip wizard prompts."),
        ] = False,
        validate: Annotated[
            bool,
            typer.Option("--validate/--no-validate", help="Run row count reconciliation after migration."),
        ] = True,
        validate_checksums: Annotated[
            bool,
            typer.Option("--validate-checksums", help="Also compare MD5 checksums (slower)."),
        ] = False,
        fail_fast: Annotated[
            bool,
            typer.Option("--fail-fast", help="Stop on first table failure."),
        ] = False,
        unsigned_checks: Annotated[
            bool,
            typer.Option("--unsigned-checks/--no-unsigned-checks", help="Generate CHECK >= 0 for UNSIGNED columns."),
        ] = False,
        enum_as_type: Annotated[
            bool,
            typer.Option(
                "--enum-as-type/--enum-as-check",
                help="Convert ENUM to PG CREATE TYPE (default: TEXT+CHECK).",
            ),
        ] = False,
        skip_tables: Annotated[
            list[str],
            typer.Option("--skip-table", help="Table(s) to skip (repeatable)."),
        ] = [],
        only_tables: Annotated[
            list[str],
            typer.Option("--only-table", help="Migrate only these table(s) (repeatable)."),
        ] = [],
        report_file: Annotated[
            Path | None,
            typer.Option("--report", "-r", help="Write JSON migration report to this file."),
        ] = None,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Migrate one or more databases from MariaDB to PostgreSQL.

    Requires both MY_* (source) and PG_* (target) credentials in .backup config.
    Use --dry-run to preview the migration plan without writing any data.
    """
    from .config import DbType
    from .migrate import run_migrate

    all_cfgs = load_all_configs(config_file)
    src_cfgs = [c for c in all_cfgs if c.db_type == DbType.MARIADB]
    dst_cfgs = [c for c in all_cfgs if c.db_type == DbType.POSTGRES]

    if not src_cfgs:
        console.print(
            "[bold red]ERROR:[/] No MariaDB source config found. "
            "Set MY_USERNAME and MY_HOST in .backup (multi-engine mode)."
        )
        raise typer.Exit(code=1)
    if not dst_cfgs:
        console.print(
            "[bold red]ERROR:[/] No PostgreSQL target config found. "
            "Set PG_USERNAME and PG_HOST in .backup (multi-engine mode)."
        )
        raise typer.Exit(code=1)

    run_migrate(
        src_cfg=src_cfgs[0],
        dst_cfg=dst_cfgs[0],
        interactive=not no_interactive,
        source_db=source_db,
        target_db=target_db,
        target_schema=target_schema,
        batch_size=batch_size,
        parallel=parallel,
        dry_run=dry_run,
        validate=validate,
        validate_checksums=validate_checksums,
        fail_fast=fail_fast,
        unsigned_checks=unsigned_checks,
        enum_as_type=enum_as_type,
        skip_tables=list(skip_tables) or None,
        only_tables=list(only_tables) or None,
        report_file=report_file,
    )


if __name__ == "__main__":
    app()
