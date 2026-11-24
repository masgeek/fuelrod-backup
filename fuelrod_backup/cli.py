"""Typer CLI entry point: `fuelrod-backup backup` and `fuelrod-backup restore`."""

from __future__ import annotations

import sys
from importlib.metadata import version as _pkg_version
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel

from .config import DbType, load_all_configs, load_config
from .engine_select import load_config_choosing_engine as _choose_cfg
from .notify import EXIT_ABORTED, EXIT_FAILURE, EXIT_USAGE
from .output import emit, enable_json, error, json_enabled, register

_VERSION = _pkg_version("fuelrod-backup")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"fuelrod-backup {_VERSION}")
        raise typer.Exit()


app = typer.Typer(
    name="fuelrod-backup",
    help="Interactive database backup and restore tool (PostgreSQL, MariaDB, MSSQL).",
    add_completion=False,
)


def _force_utf8_output() -> None:
    """
    Make stdout/stderr UTF-8 tolerant before anything prints.

    Windows consoles still default to a legacy code page (cp1252), where Rich's
    box-drawing and status glyphs (✓, ✗, ─) raise UnicodeEncodeError mid-render.
    Reconfiguring to utf-8 with errors="replace" makes degraded output
    possible instead of a traceback.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except (ValueError, OSError):  # already-detached or non-reconfigurable stream
                pass


@app.callback()
def _main(
    version: Annotated[
        bool,
        typer.Option("--version", "-V", callback=_version_callback, is_eager=True, help="Show version and exit."),
    ] = False,
    as_json: Annotated[bool, _JSON_OPT] = False,
) -> None:
    _force_utf8_output()
    _enable_json(as_json)
console = register(Console())

# Reusable option definitions
_CONFIG_OPT = typer.Option("--config", "-c", help="Path to .backup or .env config file.", exists=True, dir_okay=False)
_DOCKER_OPT = typer.Option("--docker/--no-docker", help="Override USE_DOCKER from config (highest priority).")
_VERBOSE_OPT = typer.Option(
    "--verbose", "-v", count=True,
    help="Show every rclone command and file as it happens (-v), or rclone's debug output (-vv).",
)
_RCLONE_OPT = typer.Option("--rclone", help="Path to the rclone executable, e.g. D:/tools/rclone.exe (RCLONE_CMD).")
_DB_TYPE_OPT = typer.Option("--db-type", "-t", help="Database engine: postgres | mariadb | mssql.")
_JSON_OPT = typer.Option(
    "--json",
    help="Emit a machine-readable JSON document on stdout instead of human output.",
)


def _enable_json(as_json: bool) -> bool:
    """
    Switch the process into JSON output mode; returns whether it is active.

    Sticky by design: a global `--json` before the subcommand must not be undone
    by a subcommand that defaults to False, so this only ever turns the mode on.
    """
    if as_json:
        enable_json(True)
    return json_enabled()


def _apply_docker_override(cfg, use_docker: bool | None) -> None:
    """Apply --docker/--no-docker CLI flag if explicitly provided."""
    if use_docker is not None:
        cfg.use_docker = use_docker


def _fail(msg: str, code: int) -> None:
    """Report a fatal message on stderr and exit with the contract's code."""
    if json_enabled():
        error(msg)
    else:
        console.print(f"[bold red]ERROR:[/] {msg}")
    raise typer.Exit(code=code)


def _validate_db_type(db_type: str | None) -> None:
    if db_type is not None and db_type.lower() not in (e.value for e in DbType):
        _fail(f"Unknown --db-type '{db_type}'. Choose: postgres, mariadb, mssql", EXIT_USAGE)
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
        encrypt: Annotated[
            bool | None,
            typer.Option(
                "--encrypt/--no-encrypt",
                help="Encrypt artifacts with age/gpg after compression (overrides ENCRYPT).",
            ),
        ] = None,
        databases: Annotated[
            list[str],
            typer.Option("--db", "-d", help="Database(s) to back up (repeatable). Default: all."),
        ] = [],
        schemas: Annotated[
            list[str],
            typer.Option("--schema", "-s", help="Schemas to back up (repeatable). Only applies to supported engines."),
        ] = [],
        use_docker: Annotated[bool | None, _DOCKER_OPT] = None,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Back up one or more databases (postgres | mariadb | mssql).

    Use --all-engines to back up every engine whose PG_/MY_/MS_ credentials
    are configured, running all engines in parallel.
    """
    from .backup import run_backup, run_parallel_backup

    _validate_db_type(db_type)
    _enable_json(as_json)
    if all_engines:
        configs = load_all_configs(config_file)
        for cfg in configs:
            _apply_docker_override(cfg, use_docker)
        if encrypt is not None:
            for c in configs:
                c.encrypt = encrypt
        run_parallel_backup(
            configs,
            databases=list(databases) or None,
            schemas=list(schemas),

            compress=compress,
            keep_days=keep_days,
        )
        return

    cfg = _choose_cfg(config_file, db_type, interactive=not no_interactive and not as_json, loader=load_config)
    _apply_docker_override(cfg, use_docker)
    if encrypt is not None:
        cfg.encrypt = encrypt
    run_backup(
        cfg,
        interactive=not no_interactive,
        databases=list(databases) or None,
        compress=compress,
        keep_days=keep_days,
        schemas=list(schemas),
    )


@app.command()
def restore(
        no_interactive: Annotated[
            bool,
            typer.Option(
                "--no-interactive", "-n",
                help="Skip all wizard prompts. Requires --db; needs --yes to execute.",
            ),
        ] = False,
        project: Annotated[
            str | None,
            typer.Option(
                "--project", "-p",
                help="Project/group folder under BASE_DIR (omit when BASE_DIR/<engine> exists).",
            ),
        ] = None,
        databases: Annotated[
            list[str],
            typer.Option("--db", "-d", help="Database(s) to restore (repeatable)."),
        ] = [],
        file_name: Annotated[
            str | None,
            typer.Option("--file", "-f", help="Backup file name inside the database folder. Default: latest."),
        ] = None,
        target: Annotated[
            str | None,
            typer.Option("--target-db", help="Restore into this database name (default: same as --db)."),
        ] = None,
        scope: Annotated[
            str,
            typer.Option("--scope", help="full | schema | data (PostgreSQL only)."),
        ] = "full",
        clean: Annotated[
            bool,
            typer.Option("--clean/--no-clean", help="Drop existing objects before restoring."),
        ] = True,
        schemas: Annotated[
            list[str],
            typer.Option("--schema", help="Restore only this schema (repeatable)."),
        ] = [],
        no_owner: Annotated[
            bool,
            typer.Option("--no-owner", help="Restore with --no-owner --no-privileges (skip ownership)."),
        ] = False,
        dry_run: Annotated[
            bool,
            typer.Option("--dry-run", help="Print the restore plan and exit without changing anything."),
        ] = False,
        yes: Annotated[
            bool,
            typer.Option("--yes", "-y", help="Execute without the final confirmation (non-interactive)."),
        ] = False,
        chain: Annotated[
            bool,
            typer.Option(
                "--incremental-chain",
                help="After restoring the chosen base dump, apply every newer _inc dump in order.",
            ),
        ] = False,
        force: Annotated[
            bool,
            typer.Option("--force", help="Restore even if the target database has active connections."),
        ] = False,
        safety_dump: Annotated[
            bool,
            typer.Option("--safety-dump", help="Dump the existing target database before overwriting it."),
        ] = False,
        all_engines: Annotated[
            bool,
            typer.Option(
                "--all-engines", "-a",
                help="Run the restore wizard for each engine configured with PG_/MY_/MS_ prefixes, one after another.",
            ),
        ] = False,
        use_docker: Annotated[bool | None, _DOCKER_OPT] = None,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Restore a database from a dump file (postgres | mariadb | mssql).

    Interactive by default. With --no-interactive every prompt is replaced by
    an explicit option: --db is required (--project only for a project-folder layout),
    --file defaults to the
    latest backup, and --yes is required to actually execute (otherwise the
    plan is printed and nothing is changed).
    """
    from .restore import run_restore

    _validate_db_type(db_type)
    _enable_json(as_json)
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
            run_restore(cfg, interactive=not no_interactive)
        return

    if not databases:
        if no_interactive:
            _fail("--db is required with --no-interactive.", EXIT_USAGE)
    elif len(databases) > 1 and not no_interactive:
        _fail("Interactive restore handles one database at a time.", EXIT_USAGE)

    cfg = _choose_cfg(config_file, db_type, interactive=not no_interactive and not as_json, loader=load_config)
    _apply_docker_override(cfg, use_docker)

    if len(databases) > 1:
        # Multi-database automation: one target database per source.
        for db in databases:
            run_restore(
                cfg,
                interactive=False,
                project=project,
                database=db,
                file_name=file_name,
                target=target,
                scope=scope,
                clean=clean,
                schemas=list(schemas),
                no_owner=no_owner,
                dry_run=dry_run,
                yes=yes,
                incremental_chain=chain,
                force=force,
                safety_dump=safety_dump,
            )
        return

    run_restore(
        cfg,
        interactive=not no_interactive,
        project=project,
        database=databases[0] if databases else None,
        file_name=file_name,
        target=target,
        scope=scope,
        clean=clean,
        schemas=list(schemas),
        no_owner=no_owner,
        dry_run=dry_run,
        yes=yes,
        incremental_chain=chain,
        force=force,
        safety_dump=safety_dump,
    )


def _engine_cfgs(config_file: Path | None, db_type: str | None, use_docker: bool | None = None) -> list:
    """One config per configured engine, or just *db_type* when given. Never prompts."""
    if db_type is not None:
        cfgs = [load_config(config_file, db_type_override=db_type)]
    else:
        cfgs = load_all_configs(config_file)
    for c in cfgs:
        _apply_docker_override(c, use_docker)
    return cfgs


@app.command("list")
def list_cmd(
        engine: Annotated[
            str | None,
            typer.Option("--engine", "-e", help="Restrict to one engine: postgres | mariadb | mssql."),
        ] = None,
        database: Annotated[
            str | None,
            typer.Option("--db", "-d", help="Restrict to one database folder."),
        ] = None,
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """List backups under BASE_DIR per engine and database (size, age, encrypted, incremental)."""
    from .catalog import list_backups
    from .fileutil import human_size

    _enable_json(as_json)
    _validate_db_type(engine)
    cfg = load_config(config_file)
    if not cfg.base_dir or not Path(cfg.base_dir).is_dir():
        _fail(f"Backup directory not found: {cfg.base_dir}", EXIT_FAILURE)
    entries = list_backups(cfg.base_dir, [engine] if engine else None, database)
    if json_enabled():
        emit({"command": "list", "base_dir": cfg.base_dir, "count": len(entries),
                     "backups": [e.to_dict() for e in entries]})
        return
    if not entries:
        console.print(f"[yellow]No backups found under {cfg.base_dir}.[/]")
        return
    import time

    from rich.table import Table

    table = Table(show_header=True, header_style="bold")
    for col in ("Engine", "Database", "File", "Size", "Age", "Flags"):
        table.add_column(col, overflow="fold")
    for e in entries:
        flags = ",".join(f for f, on in (("enc", e.encrypted), ("gz", e.compressed), ("inc", e.incremental)) if on)
        size = human_size(e.size)
        age = f"{(time.time() - e.mtime) / 86400:.1f}d"
        table.add_row(e.engine, e.database, Path(e.file).name, size, age, flags)
    console.print(table)
    console.print(f"  {len(entries)} backup(s)")


@app.command("schedule")
def schedule_cmd(
        job: Annotated[
            str,
            typer.Option("--job", "-j", help="What to schedule: backup, pull, push, quota, verify or doctor."),
        ] = "backup",
        cron: Annotated[
            str | None,
            typer.Option("--cron", help="Cron expression, e.g. '0 2 * * *' (default depends on the job)."),
        ] = None,
        log_file: Annotated[
            str | None, typer.Option("--log", help="File the job appends its output to.")
        ] = None,
        extra: Annotated[
            list[str],
            typer.Option("--arg", "-a", help="Extra option for the job, e.g. --arg '--latest 5' (repeatable)."),
        ] = [],
        apply: Annotated[bool, typer.Option("--apply", help="Install the job instead of only printing it.")] = False,
        list_jobs: Annotated[bool, typer.Option("--list", "-l", help="List the installed jobs.")] = False,
        remove: Annotated[bool, typer.Option("--remove", help="Remove the installed entry for --job.")] = False,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Schedule backup, pull, push, quota, verify or doctor runs (cron / Windows Task Scheduler).

    Each job has its own entry, so `--job backup`, `--job push` and `--job pull` can all be installed and
    are replaced independently. Prints the crontab line or schtasks command; --apply installs it,
    --list shows what is installed and --remove deletes one.

    Example: fuelrod-backup schedule --job pull --cron "30 3 * * *" --apply
    """
    import subprocess

    from . import schedule as sched

    if job not in sched.JOBS:
        _fail(f"Unknown job '{job}'. Choose from: {', '.join(sched.JOBS)}", EXIT_USAGE)
    if list_jobs:
        jobs = sched.list_jobs()
        if json_enabled():
            emit({"command": "schedule", "jobs": [j.__dict__ for j in jobs]})
        elif not jobs:
            console.print("[yellow]No fuelrod-backup jobs are scheduled.[/]")
        else:
            for j in jobs:
                console.print(f"  [bold]{j.job:<8}[/] {j.when}  [dim]{j.command}[/]", soft_wrap=True)
        return
    if remove:
        if sched.remove_job(job):
            console.print(f"[green]Removed the scheduled {job} job.[/]")
        else:
            _fail(f"No scheduled {job} job was found.", EXIT_FAILURE)
        return

    _validate_db_type(db_type)
    try:
        command = sched.build_command(job, db_type, str(config_file.resolve()) if config_file else None, list(extra))
        text, argv = sched.render(job, cron or sched.default_cron(job), command, log_file or sched.default_log(job))
    except ValueError as exc:
        _fail(str(exc), EXIT_USAGE)
    console.print(text, markup=False, soft_wrap=True)
    if not apply:
        console.print()
        console.print("Re-run with --apply to install it.")
        return
    try:
        Path(log_file or sched.default_log(job)).parent.mkdir(parents=True, exist_ok=True)
        if argv:
            subprocess.run(argv, check=True)  # noqa: S603
        else:
            sched.install_cron(job, text)
    except (subprocess.CalledProcessError, OSError) as exc:
        _fail(f"Could not install the schedule: {exc}", EXIT_FAILURE)
    console.print(f"[green]Scheduled the {job} job.[/]")


@app.command("doctor")
def doctor(
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        skip_connection: Annotated[
            bool,
            typer.Option("--skip-connection", help="Skip the live database connection test."),
        ] = False,
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Run pre-flight checks and report everything that is not ready.

    Verifies the config file, credentials, BASE_DIR writability and free disk
    space, required external binaries (pg_dump, mariadb-dump, ...), Docker and
    the service state, the database connection, and rclone/n8n configuration.
    Exits non-zero if any check fails.
    """
    from .doctor import run_doctor

    _enable_json(as_json)
    _validate_db_type(db_type)
    cfgs = _engine_cfgs(config_file, db_type)
    if len(cfgs) == 1:
        code = run_doctor(cfgs[0], skip_connection=skip_connection)
    else:
        payloads: list = []
        code = 0
        for c in cfgs:
            code = run_doctor(c, skip_connection=skip_connection, collect=payloads if json_enabled() else None) or code
        if json_enabled():
            emit({"command": "doctor", "status": "fail" if code else "ok", "engines": payloads})
    if code:
        raise typer.Exit(code=code)


@app.command("verify")
def verify(
        engine: Annotated[
            str | None,
            typer.Option("--engine", "-e", help="Restrict to one engine folder: postgres | mariadb | mssql."),
        ] = None,
        project: Annotated[
            str | None,
            typer.Option("--project", "-p", help="Restrict to one project folder under BASE_DIR."),
        ] = None,
        databases: Annotated[
            list[str],
            typer.Option("--db", "-d", help="Restrict to these database folders (repeatable)."),
        ] = [],
        latest: Annotated[
            bool,
            typer.Option("--latest", "-l", help="Verify only the newest backup per database."),
        ] = False,
        checksum: Annotated[
            bool,
            typer.Option("--checksum", "-k", help="Also compute a SHA-256 for each artifact."),
        ] = False,
        deep: Annotated[
            bool,
            typer.Option("--deep", help="Fully decompress/CRC every archive (slow on large trees)."),
        ] = False,
        parallel: Annotated[
            int | None,
            typer.Option("--parallel", "-j", help="Concurrent workers. Default: min(8, CPU count)."),
        ] = None,
        restore_test: Annotated[
            bool,
            typer.Option(
                "--restore-test",
                help="Restore the newest full backup per database into a throwaway database and sanity-check it.",
            ),
        ] = False,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Verify that backup files on disk are actually restorable.

    Checks each artifact under BASE_DIR: gzip/zip integrity, SQL headers,
    SQL Server backup headers, and — when pg_restore is available — the
    PostgreSQL table of contents. Exits non-zero if any artifact fails.
    """
    from .verify import run_verify

    _enable_json(as_json)
    _validate_db_type(db_type)
    if restore_test:
        from .restoretest import run_restore_test

        cfg = _choose_cfg(config_file, engine or db_type, interactive=not as_json, loader=load_config)
        code = run_restore_test(cfg, database=databases[0] if databases else None)
        if code:
            raise typer.Exit(code=code)
        return
    cfg = load_config(config_file, db_type_override=db_type)
    if engine:
        _validate_db_type(engine)
    code = run_verify(
        cfg,
        engine=engine,
        project=project,
        database=databases[0] if databases else None,
        latest_only=latest,
        checksum=checksum,
        deep=deep,
        parallel=parallel,
    )
    if code:
        raise typer.Exit(code=code)


@app.command("drop")
def drop(
        disconnect_only: Annotated[bool, typer.Option(
            "--disconnect-only",
            help="Terminate all connections to the database instead of dropping it.",
        )] = False,
        db: Annotated[str | None, typer.Option(
            "--db", "-d",
            help="Target database — skip the database prompt (used with --disconnect-only).",
        )] = None,
        use_docker: Annotated[bool | None, _DOCKER_OPT] = None,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Interactively drop a database or schema.

    Kills all active connections before dropping a database.
    Schema drop uses CASCADE (removes all objects inside the schema).
    Requires typing the name to confirm — operation is irreversible.

    Use --disconnect-only to terminate connections WITHOUT dropping anything.
    """
    from .drop import run_drop

    _enable_json(as_json)
    _validate_db_type(db_type)
    cfg = _choose_cfg(config_file, db_type, interactive=not as_json, loader=load_config)
    _apply_docker_override(cfg, use_docker)
    run_drop(cfg, disconnect_only=disconnect_only, db_name=db)


@app.command("create")
def create(
        as_json: Annotated[bool, _JSON_OPT] = False,
        use_docker: Annotated[bool | None, _DOCKER_OPT] = None,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Interactively create a database or schema.

    Creates a new database, or a new PostgreSQL schema inside
    an existing database. Name confirmation is required.
    """
    from .create import run_create

    _enable_json(as_json)
    _validate_db_type(db_type)
    cfg = _choose_cfg(config_file, db_type, interactive=not as_json, loader=load_config)
    _apply_docker_override(cfg, use_docker)
    run_create(cfg)


@app.command("test")
def test_connection(
        use_docker: Annotated[bool | None, _DOCKER_OPT] = None,
        db_type: Annotated[str | None, _DB_TYPE_OPT] = None,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
        as_json: Annotated[bool, _JSON_OPT] = False,
) -> None:
    """Test the connection for every configured engine and print resolved settings.

    Use --db-type to test only one engine.
    """
    _validate_db_type(db_type)
    _enable_json(as_json)
    cfgs = _engine_cfgs(config_file, db_type, use_docker)

    results = [_test_one(c, use_docker, as_json) for c in cfgs]
    if as_json:
        import json
        print(json.dumps(results[0] if len(results) == 1 else {"engines": results}, indent=2))
    if not all(r["connection_successful"] for r in results):
        raise typer.Exit(code=1)


def _test_one(cfg, use_docker: bool | None, as_json: bool) -> dict:
    """Test one engine's connection; prints human output unless *as_json*. Returns the JSON payload."""
    from .adapters import get_adapter

    if not as_json:
        pass_hint = (
            f"{'*' * min(len(cfg.password), 6)}  ({len(cfg.password)} chars)"
            if cfg.password else "[red]NOT SET[/]"
        )
        source = str(cfg.config_source) if cfg.config_source else "[red]none found — using defaults only[/]"
        docker_override = " [yellow](CLI override)[/]" if use_docker is not None else ""

        console.print()
        console.rule(f"[bold cyan]{cfg.db_type.value}[/]")
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

    payload = {
        "config_source": str(cfg.config_source) if cfg.config_source else None,
        "db_type": cfg.db_type.value,
        "mode": "docker" if cfg.use_docker else "direct",
        "host": f"{cfg.host}:{cfg.port}",
        "user": cfg.user,
        "password_set": bool(cfg.password),
        "backup_dir": str(cfg.backup_dir),
    }
    try:
        get_adapter(cfg).check_connection()
    except Exception as exc:  # noqa: BLE001 — any failure is a failed test, reported per engine
        if not as_json:
            console.print(f"[bold red]✗ Connection failed:[/] {exc}")
        return {**payload, "connection_successful": False, "error": str(exc)}
    if not as_json:
        console.print("[bold green]✓ Connection successful.[/]")
    return {**payload, "connection_successful": True}


@app.command("init")
def init_config(
        output: Annotated[
            Path,
            typer.Option("--output", "-o", help="Path for the config file.", dir_okay=False),
        ] = None,
        section: Annotated[
            list[str],
            typer.Option(
                "--section", "-s",
                help="Configure only this group of settings (repeatable, or 'all'): "
                     "storage, backup, encryption, notifications, push, quota, pull, rclone, n8n.",
            ),
        ] = [],
        advanced: Annotated[
            bool,
            typer.Option("--advanced", help="Skip the connection wizard and pick which setting groups to review."),
        ] = False,
        list_settings: Annotated[
            bool,
            typer.Option("--list", "-l", help="List every setting with its current value, default and description."),
        ] = False,
        as_json: Annotated[bool, _JSON_OPT] = False,
) -> None:
    """Create or update a .backup config file interactively.

    The main wizard covers the connection and storage basics and then offers the advanced settings
    (retention, hooks, notifications, encryption, push/pull, quota, rclone, n8n). Re-running it only
    changes what you answer; every other line of the file is kept.
    """
    from . import prompt as q
    from .config import _find_config_file, _parse_env_file, user_config_dir
    from .settings import SECTIONS

    if output is None:
        output = user_config_dir() / ".backup"
    output = output.resolve()
    updating = output.exists()

    _enable_json(as_json)
    if list_settings:
        _print_settings(output, as_json)
        return
    if section or advanced:
        names = None if not section else (list(SECTIONS) if "all" in section else list(dict.fromkeys(section)))
        unknown = [n for n in names or [] if n not in SECTIONS]
        if unknown:
            _fail(f"Unknown section(s): {', '.join(unknown)}. Choose from: {', '.join(SECTIONS)} or all.", EXIT_USAGE)
        _run_advanced(q, output, names)
        return
    if as_json:
        import json
        cfg = load_config(None)
        print(json.dumps({
            "output_file": str(output),
            "proposed_keys": {
                "DB_TYPE": cfg.db_type.value,
                "DB_USERNAME": cfg.user,
                "DB_HOST": cfg.host,
                "DB_PORT": str(cfg.port),
                "BASE_DIR": cfg.base_dir,
                "KEEP_DAYS": cfg.days_to_keep,
            }
        }, indent=2))
        return

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

    if output.is_file() and q.confirm(
        "Configure an rclone remote to pull backups from?", default=False
    ).ask():
        _save_pull_source(q, output)
    if output.is_file() and q.confirm(
        "Configure an rclone remote to push backups to?", default=False
    ).ask():
        _save_remote_target(q, output, "PUSH")
    if output.is_file() and q.confirm(
        "Review the advanced settings now? (retention, hooks, notifications, encryption, quota, ...)", default=False,
    ).ask():
        _run_advanced(q, output, None)


# ──────────────────────────────────────────────────────────────────────────────
#  init helpers
# ──────────────────────────────────────────────────────────────────────────────

def _init_generic_remote(q, output: Path, updating: bool, existing_raw: dict[str, str]) -> dict[str, str]:
    """Collect a generic rclone remote string."""
    console.print()
    console.rule("[bold cyan]Off-site Generic Remote[/]")
    ex_remote = existing_raw.get("REMOTE", "")
    remote = q.text(
        "rclone remote spec (REMOTE)\n"
        "Example: 'sftp:user@host:/path' or 'b2:bucket/folder' or '/mnt/backups'",
        default=ex_remote
    ).ask() or ex_remote
    return {"REMOTE": remote} if remote else {}


def _ask_setting(q, setting, current: str | None) -> str | None:
    """Prompt for one setting. Returns the normalised value, or None to leave it untouched."""
    from .settings import normalise

    label = f"{setting.help} ({setting.key})"
    shown = current if current is not None else setting.default
    if setting.kind == "bool":
        is_on = shown.strip().lower() in ("true", "1", "yes", "on")
        return "true" if q.confirm(label, default=is_on).ask() else "false"
    if setting.kind == "choice":
        picked = q.select(
            label,
            choices=[q.Choice(c or "(none)", value=c) for c in setting.choices],
            default=shown if shown in setting.choices else setting.choices[0],
        ).ask()
        return picked if picked is not None else None
    if setting.kind == "secret":
        value = q.password(f"{label} [blank = keep current]").ask()
        return value or None
    for _attempt in range(3):
        raw = q.text(label, default=shown).ask()
        if raw is None:
            return None
        try:
            return normalise(setting, raw)
        except ValueError as exc:
            console.print(f"  [red]{exc}[/]")
    console.print("  [yellow]Too many invalid answers; keeping the current value.[/]")
    return None


def _run_advanced(q, output: Path, sections: list[str] | None) -> None:
    """Walk the advanced setting groups and merge the answers into the config file."""
    from . import settings as st
    from .config import _parse_env_file

    existing_raw = _parse_env_file(output) if output.is_file() else {}
    if sections is None:
        sections = q.checkbox(
            "Which settings do you want to review?",
            choices=[q.Choice(title=title, value=name) for name, title in st.SECTIONS.items()],
        ).ask() or []
        if not sections:
            console.print("[yellow]Nothing selected.[/]")
            return

    values = dict(existing_raw)
    changes: dict[str, str] = {}
    removals: set[str] = set()
    for name in sections:
        console.print()
        console.rule(f"[bold cyan]{st.SECTIONS[name]}[/]")
        for setting in st.in_section(name):
            if not st.applies(setting, values):
                continue
            answer = _ask_setting(q, setting, values.get(setting.key))
            if answer is None:
                continue
            present = setting.key in existing_raw
            if answer == "":
                if present:
                    removals.add(setting.key)
                    changes.pop(setting.key, None)
                    values.pop(setting.key, None)
                continue
            if answer == existing_raw.get(setting.key) or (not present and answer == setting.default):
                values[setting.key] = answer
                changes.pop(setting.key, None)
                continue
            changes[setting.key] = answer
            values[setting.key] = answer
            removals.discard(setting.key)

    if not changes and not removals:
        console.print("\n[yellow]No changes.[/]")
        return
    console.print()
    for key, value in changes.items():
        shown = "***" if st.BY_KEY[key].kind == "secret" else value
        console.print(f"  [green]+[/] {key}={shown}")
    for key in sorted(removals):
        console.print(f"  [red]-[/] {key}")
    if not q.confirm(f"Save these changes to {output}?", default=True).ask():
        console.print("[yellow]Aborted.[/]")
        raise typer.Exit(EXIT_ABORTED)
    _write_config(output, [f"{k}={v}" for k, v in changes.items()], True, remove=removals)


def _print_settings(output: Path, as_json: bool) -> None:
    """`init --list`: every setting, where it is configured, its current value and its default."""
    from rich.table import Table

    from . import settings as st
    from .config import _parse_env_file

    raw = _parse_env_file(output) if output.is_file() else {}
    rows = []
    for s in st.SETTINGS:
        rows.append({
            "key": s.key, "group": s.group, "type": s.kind, "default": s.default,
            "value": "***" if s.kind == "secret" and s.key in raw else raw.get(s.key),
            "configure_with": f"init --section {s.group}" if s.advanced else f"{s.wizard}",
            "description": s.help,
        })
    if as_json:
        import json

        print(json.dumps({"config_file": str(output), "settings": rows}, indent=2))
        return
    table = Table(show_header=True, header_style="bold")
    for col in ("Key", "Group", "Current", "Default", "Set with", "Description"):
        table.add_column(col, overflow="fold")
    for r in rows:
        table.add_row(
            r["key"], r["group"], r["value"] if r["value"] is not None else "[dim](not set)[/]", r["default"] or "-",
            r["configure_with"], r["description"],
        )
    console.print(f"[dim]{output}[/]")
    console.print(table)


def _update_config_keys(output: Path, keys: dict[str, str], remove: set[str] | None = None) -> None:
    """
    Merge *keys* into the config file at *output* without touching anything else.

    Existing ``KEY=value`` lines (also ``export KEY=value``) are updated in place, new keys are
    appended, and every other line — comments, blank lines, keys this wizard does not manage —
    is kept exactly as it is. Keys named in *remove* are deleted.
    """
    remove = remove or set()
    new_lines: list[str] = []
    set_keys: set[str] = set()

    for line in output.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].removeprefix("export ").strip()
            if key in remove:
                continue
            if key in keys:
                if key not in set_keys:  # a duplicated key keeps only its first line
                    new_lines.append(f"{key}={keys[key]}")
                    set_keys.add(key)
                continue
        new_lines.append(line)

    # Add new keys at the end
    missing = [key for key in keys if key not in set_keys]
    if missing:
        if new_lines and new_lines[-1].strip():
            new_lines.append("")
        new_lines.extend(f"{key}={keys[key]}" for key in missing)

    output.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    try:
        output.chmod(0o600)
    except OSError:
        pass


def _init_s3_remote(q, output: Path, updating: bool, existing_raw: dict[str, str]) -> dict[str, str]:
    from . import rclone_config

    console.print()
    console.rule("[bold cyan]Off-site S3 Target[/]")

    ex_remote = existing_raw.get("S3_REMOTE", "s3-backup")
    remote_name = q.text("rclone remote name (S3_REMOTE)", default=ex_remote).ask() or ex_remote

    ex_bucket = existing_raw.get("S3_BUCKET", "")
    bucket = q.text("S3 bucket (S3_BUCKET)", default=ex_bucket).ask() or ex_bucket
    if not bucket:
        console.print("[yellow]Skipping S3 config (bucket required).[/]")
        return {}

    ex_prefix = existing_raw.get("S3_PREFIX", "")
    prefix = q.text("S3 key prefix (S3_PREFIX)", default=ex_prefix).ask() or ex_prefix

    ex_provider = existing_raw.get("S3_PROVIDER", "")
    provider_choice = q.select(
        "S3 provider",
        choices=[q.Choice(label, v) for label, v in rclone_config.S3_PROVIDERS],
        default=ex_provider or "",
    ).ask()
    provider_val = provider_choice.value if hasattr(provider_choice, "value") else provider_choice

    ex_region = existing_raw.get("S3_REGION", "")
    region = q.text("S3 region (S3_REGION)", default=ex_region).ask() or ex_region

    ex_endpoint = existing_raw.get("S3_ENDPOINT", "")
    endpoint = q.text("S3 endpoint (S3_ENDPOINT) [leave blank for AWS]", default=ex_endpoint).ask() or ex_endpoint

    console.print("\n  S3 credentials can be stored in rclone config or .backup file.")
    use_env_auth = q.confirm("Store keys in rclone config? (secrets will NOT be in .backup)", default=True).ask()

    ex_access = existing_raw.get("S3_ACCESS_KEY_ID", "")
    ex_secret = existing_raw.get("S3_SECRET_ACCESS_KEY", "")

    access_key = q.text("Access Key ID (S3_ACCESS_KEY_ID)", default=ex_access).ask() or ex_access
    secret_key = q.password("Secret Access Key (S3_SECRET_ACCESS_KEY)", default=ex_secret).ask() or ex_secret

    try:
        rclone_config.write_remote(
            remote_name,
            "s3",
            rclone_config.s3_options(access_key, secret_key, provider_val, region, endpoint, not use_env_auth)
        )
        console.print(f"[bold green]✓[/] rclone remote {remote_name} configured.")
    except Exception as exc:
        console.print(f"[bold red]✗[/] rclone config failed: {exc}")

    return {
        "S3_REMOTE": remote_name,
        "S3_BUCKET": bucket,
        "S3_PREFIX": prefix,
        "S3_PROVIDER": provider_val,
        "S3_REGION": region,
        "S3_ENDPOINT": endpoint,
        "S3_ACCESS_KEY_ID": access_key,
        "S3_SECRET_ACCESS_KEY": secret_key,
    }

def _write_config(output: Path, lines: list[str], updating: bool, remove: set[str] | None = None) -> None:
    """
    Save the wizard's *lines*. A new file is written in full. An existing file is merged instead:
    the wizard's keys are updated in place and everything it does not ask about (NOTIFY_*, RETENTION,
    PULL_REMOTE, PUSH_*, ENCRYPT_*, hooks, comments ...) is preserved. The previous file is kept as
    ``<name>.bak``. Keys in *remove* are deleted (for example an engine you just deselected).
    """
    import shutil

    output.parent.mkdir(parents=True, exist_ok=True)
    if output.is_file() and output.read_text(encoding="utf-8").strip():
        shutil.copy2(output, output.with_name(output.name + ".bak"))
        managed: dict[str, str] = {}
        for line in lines:
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key, value = stripped.split("=", 1)
                managed[key.strip()] = value
        _update_config_keys(output, managed, remove)
    else:
        output.write_text("\n".join(lines) + "\n", encoding="utf-8")
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

    # Off-site sync
    if q.confirm("\nConfigure an off-site sync remote now?", default=False).ask():
        remote_type = q.select(
            "Remote configuration type",
            choices=[
                q.Choice("Guided S3 Setup", value="s3"),
                q.Choice("Generic rclone remote", value="generic"),
            ],
            default="s3",
        ).ask()

        if remote_type == "s3":
            s3_keys = _init_s3_remote(q, output, updating, existing_raw)
            for k, v in s3_keys.items():
                lines.append(f"{k}={v}")
            if s3_keys:
                lines.append("")
        else:
            gen_keys = _init_generic_remote(q, output, updating, existing_raw)
            for k, v in gen_keys.items():
                lines.append(f"{k}={v}")
            if gen_keys:
                lines.append("")

    # An engine that was configured before but is no longer selected must really go away,
    # otherwise --all-engines would still pick up its PG_/MY_/MS_ credentials.
    dropped = {
        f"{prefixes[engine]}{short}"
        for engine in prefixes
        if engine not in selected_engines
        for short in ("USERNAME", "PASSWORD", "HOST", "PORT", "SERVICE", "USE_DOCKER")
        if f"{prefixes[engine]}{short}" in existing_raw
    }
    _write_config(output, lines, updating, remove=dropped)


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

    # Off-site sync
    if q.confirm("\nConfigure an off-site sync remote now?", default=False).ask():
        remote_type = q.select(
            "Remote configuration type",
            choices=[
                q.Choice("Guided S3 Setup", value="s3"),
                q.Choice("Generic rclone remote", value="generic"),
            ],
            default="s3",
        ).ask()

        if remote_type == "s3":
            s3_keys = _init_s3_remote(q, output, updating, existing_raw)
            for k, v in s3_keys.items():
                lines.append(f"{k}={v}")
            if s3_keys:
                lines.append("")
        else:
            gen_keys = _init_generic_remote(q, output, updating, existing_raw)
            for k, v in gen_keys.items():
                lines.append(f"{k}={v}")
            if gen_keys:
                lines.append("")

    _write_config(output, lines, updating)


@app.command("sync-init")
def sync_init(
        output: Annotated[
            Path,
            typer.Option("--output", "-o", help="Path for the config file.", dir_okay=False),
        ] = None,
) -> None:
    """Configure off-site sync settings without running a full init wizard."""
    from . import prompt as q
    from .config import _parse_env_file, user_config_dir

    if output is None:
        output = user_config_dir() / ".backup"
    output = output.resolve()

    existing_raw = _parse_env_file(output) if output.is_file() else {}

    remote_type = q.select(
        "Remote configuration type",
        choices=[
            q.Choice("Guided S3 Setup", value="s3"),
            q.Choice("Generic rclone remote", value="generic"),
        ],
        default="s3",
    ).ask()

    if remote_type == "s3":
        keys = _init_s3_remote(q, output, output.exists(), existing_raw)
    else:
        keys = _init_generic_remote(q, output, output.exists(), existing_raw)

    if keys:
        _update_config_keys(output, keys)
        console.print(f"\n[bold green]✓[/] Sync settings updated in [bold]{output}[/]")
    else:
        console.print("\n[yellow]No changes made to config.[/]")


def _init_rclone_target(
    q, existing_raw: dict[str, str], prefix: str, names: list[str], dir_prompt: str, *, must_exist: bool,
    preselected: str | None = None,
) -> dict[str, str]:
    """Collect <prefix>_REMOTE from the rclone remotes already configured on this machine."""
    from .pull import _lsf_dirs
    from .storage import StorageError

    ex_name, _, ex_sub = existing_raw.get(f"{prefix}_REMOTE", "").partition(":")
    name = preselected or q.select(
        "rclone remote",
        choices=[q.Choice(f"{n}:", value=n) for n in names],
        default=ex_name if ex_name in names else names[0],
    ).ask()
    if not name:
        return {}
    sub = (q.text(dir_prompt, default=ex_sub).ask() or "").strip().strip("/")
    spec = f"{name}:{sub}"

    if q.confirm("Test the connection now?", default=True).ask():
        try:
            folders = _lsf_dirs(spec, [])
        except StorageError as exc:
            if not must_exist and "not found" in str(exc).lower():
                console.print("  [green]✓ Connected.[/] The directory does not exist yet; it is created on first push.")
            else:
                console.print(f"  [bold red]✗ Connection test failed:[/] {exc}")
                if not q.confirm("Save these settings anyway?", default=False).ask():
                    return {}
        else:
            found = ", ".join(folders) if folders else "no sub-folders"
            console.print(f"  [green]✓ Connected.[/] Top-level folders: [bold]{found}[/]")

    return {f"{prefix}_REMOTE": spec}


def _collect_push_destinations(q, existing_raw: dict[str, str], names: list[str]) -> dict[str, str]:
    """Collect one or several push destinations (rclone remotes and/or local folders) into PUSH_REMOTE."""
    current = [d.strip() for d in existing_raw.get("PUSH_REMOTE", "").split("|") if d.strip()]
    dests: list[str] = []
    if current:
        console.print("  Current destinations: " + ", ".join(current))
        if q.confirm("Keep them?", default=True).ask():
            dests = list(current)

    while True:
        spec = ""
        if names:
            choices = [q.Choice(f"{n}:  (rclone remote)", value=n) for n in names]
            choices.append(q.Choice("A local folder (e.g. a OneDrive or network folder)", value=""))
            pick = q.select("Add a destination", choices=choices, default=names[0]).ask()
            if pick:
                added = _init_rclone_target(
                    q, {}, "PUSH", names, "Path inside the remote to upload into (blank = remote root)",
                    must_exist=False, preselected=pick,
                )
                spec = added.get("PUSH_REMOTE", "")
        if not spec and not (names and pick):
            hint = "Folder path, e.g. D:/OneDrive/Backups" if names else "Destination (folder or rclone name:path)"
            spec = (q.text(f"{hint}, blank to finish", default="").ask() or "").strip()
            if "|" in spec:
                console.print("  [red]A destination cannot contain '|'.[/]")
                spec = ""
            if spec:
                from .storage import parse_remote

                folder = Path(spec).expanduser()
                if parse_remote(spec).provider == "local" and not folder.is_dir():
                    if q.confirm(f"{folder} does not exist. Create it?", default=True).ask():
                        folder.mkdir(parents=True, exist_ok=True)
                    else:
                        spec = ""
        if spec and spec not in dests:
            dests.append(spec)
        if not q.confirm("Add another destination?", default=False).ask():
            break
    return {"PUSH_REMOTE": "|".join(dests)} if dests else {}


def _save_remote_target(q, output: Path, prefix: str) -> None:
    from .config import _parse_env_file
    from .storage import list_remotes, rclone_available

    pull = prefix == "PULL"
    existing_raw = _parse_env_file(output) if output.is_file() else {}
    title = (
        "Pull source (download backups from an rclone remote)" if pull
        else "Push target (upload backups to an rclone remote)"
    )
    console.print()
    console.rule(f"[bold cyan]{title}[/]")

    names: list[str] = []
    if rclone_available():
        try:
            names = list_remotes()
        except SystemExit:  # list_remotes exits when rclone cannot list remotes
            names = []

    if not pull:
        keys = _collect_push_destinations(q, existing_raw, names)
    elif names:
        keys = _init_rclone_target(
            q, existing_raw, prefix, names,
            "Path inside the remote (blank = remote root)" if pull
            else "Path inside the remote to upload into (blank = remote root)",
            must_exist=pull,
        )
    else:
        console.print(
            "  [yellow]No rclone remotes found.[/] Create one with [bold]rclone config[/], then run this again."
        )
        spec = (q.text(
            "rclone remote spec (name:path), blank to cancel", default=existing_raw.get(f"{prefix}_REMOTE", ""),
        ).ask() or "").strip()
        keys = {f"{prefix}_REMOTE": spec} if spec else {}

    label = "Pull source" if pull else "Push target"
    if not keys:
        console.print()
        console.print(f"[yellow]No {label.lower()} configured.[/]")
        return
    if not output.is_file():
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("", encoding="utf-8")
    _update_config_keys(output, keys)
    console.print()
    console.print(f"[bold green]✓[/] {label} saved to [bold]{output}[/]")
    verb = "pull" if pull else "push"
    action = "download backups from" if pull else "upload backups to"
    console.print(f"  Run [bold]fuelrod-backup {verb}[/] to {action} it.")


def _save_pull_source(q, output: Path) -> None:
    _save_remote_target(q, output, "PULL")


@app.command("pull-init")
def pull_init(
        output: Annotated[
            Path,
            typer.Option("--output", "-o", help="Path for the config file.", dir_okay=False),
        ] = None,
) -> None:
    """Configure the server `pull` downloads backups from (PULL_REMOTE), without a full init."""
    from . import prompt as q
    from .config import user_config_dir

    if output is None:
        output = user_config_dir() / ".backup"
    _save_pull_source(q, output.resolve())


@app.command("push-init")
def push_init(
        output: Annotated[
            Path,
            typer.Option("--output", "-o", help="Path for the config file.", dir_okay=False),
        ] = None,
) -> None:
    """Configure the server `push` (sync) uploads backups to (PUSH_REMOTE), without a full init."""
    from . import prompt as q
    from .config import user_config_dir

    if output is None:
        output = user_config_dir() / ".backup"
    _save_remote_target(q, output.resolve(), "PUSH")


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
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Back up n8n Docker volumes (hot snapshot, no downtime)."""
    from .n8n_backup import run_n8n_backup

    _enable_json(as_json)
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
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Restore an n8n Docker volume from a backup archive."""
    from .n8n_restore import run_n8n_restore

    _enable_json(as_json)
    cfg = load_config(config_file)
    run_n8n_restore(cfg, service=service, backup_file=backup_file, dry_run=dry_run, verbose=verbose)


@app.command("quota")
def quota_cmd(
        remote: Annotated[
            list[str],
            typer.Option(
                "--remote", "-r",
                help="Destination to measure: rclone remote or local folder; repeatable "
                     "(default: PUSH_REMOTE, then REMOTE / S3_*).",
            ),
        ] = [],
        quota: Annotated[
            str | None,
            typer.Option("--quota", "-q", help="Size limit, e.g. 500M, 50G, 1T (default: PUSH_QUOTA)."),
        ] = None,
        target: Annotated[
            int | None,
            typer.Option("--target", help="Prune down to this percent of the quota (default: PUSH_QUOTA_TARGET, 90)."),
        ] = None,
        keep: Annotated[
            int | None,
            typer.Option(
                "--keep",
                help="Newest backups per database that are never pruned (default: PUSH_QUOTA_KEEP, 1).",
            ),
        ] = None,
        prune: Annotated[
            bool,
            typer.Option("--prune", help="Delete the oldest backups until usage fits (default: report only)."),
        ] = False,
        dry_run: Annotated[
            bool,
            typer.Option("--dry-run", "-n", help="With --prune: show what would be deleted without deleting."),
        ] = False,
        rclone: Annotated[str | None, _RCLONE_OPT] = None,
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Show how much of your remote quota the backups use, and optionally prune the oldest.

    Usage is the total size of the files under the remote path, compared with the quota you
    set. With --prune, the oldest files are deleted until usage is at --target percent of the
    quota; the newest --keep backups of every database are never deleted. Exits 1 when the
    remote is still over quota, so it can run from cron or a monitor (notifications fire too).
    """
    from .notify import notify, report_delivery_errors
    from .quota import enforce_quota, parse_size, print_report
    from .storage import (
        StorageError,
        _die,
        parse_remote,
        provider_args,
        rclone_available,
        use_rclone,
    )

    _enable_json(as_json)
    cfg = load_config(config_file)
    if rclone:
        use_rclone(rclone)
    if not rclone_available():
        _die("rclone not found in PATH. Install it from https://rclone.org/install/ or pass --rclone PATH.")

    from .storage import resolve_destinations

    specs = resolve_destinations(cfg, list(remote) or None)
    if not specs:
        _fail("No destination configured. Pass --remote or set PUSH_REMOTE (see `push-init`).", EXIT_USAGE)
    quota_text = (quota or cfg.push_quota).strip()
    if not quota_text:
        _fail("No quota set. Pass --quota 50G or set PUSH_QUOTA in your config.", EXIT_USAGE)
    try:
        limit = parse_size(quota_text)
        targets = [parse_remote(spec) for spec in specs]
    except ValueError as exc:
        _fail(str(exc), EXIT_USAGE)

    reports = []
    unreadable = 0
    for tgt in targets:
        location = tgt.location
        try:
            report = enforce_quota(
                location, provider_args(tgt.provider, cfg), quota=limit,
                target_pct=target if target is not None else cfg.push_quota_target,
                keep=keep if keep is not None else cfg.push_quota_keep,
                prune=prune, dry_run=dry_run,
            )
        except StorageError as exc:
            unreadable += 1
            if json_enabled():
                reports.append({"remote": location, "error": str(exc), "over_quota": True})
            else:
                console.print(f"[bold]Destination[/] {location}")
                console.print(f"  [bold red]Could not read it:[/] {exc}")
            continue
        reports.append(report)
        if not json_enabled():
            console.print(f"[bold]Destination[/] {location}")
            print_report(report, table=True)
            if report.over and not prune:
                console.print("  Run with [bold]--prune[/] to delete the oldest backups until usage fits.")

    over = unreadable > 0 or any(not isinstance(r, dict) and r.over for r in reports)
    if json_enabled():
        rows = [r if isinstance(r, dict) else r.to_dict() for r in reports]
        emit({"command": "quota", "status": "fail" if over else "ok", "destinations": rows})

    usable = [r for r in reports if not isinstance(r, dict)]
    worst = max(usable, key=lambda r: r.percent, default=None)
    summary = (
        f"{worst.percent:.0f}% of quota used on {worst.remote}" if worst else "no destination could be read"
    ) + (f" ({unreadable} unreadable)" if unreadable else "")
    errors = notify(
        status="failure" if over else "success", command="quota", cfg=cfg, summary=summary,
        exit_code=EXIT_FAILURE if over else 0,
    )
    report_delivery_errors(errors, console)
    if over:
        raise typer.Exit(code=EXIT_FAILURE)


@app.command("pull")
def pull_cmd(
        remote: Annotated[
            str | None,
            typer.Option(
                "--remote", "-r",
                help="Source rclone remote, e.g. s3:my-bucket/backups (default: REMOTE / S3_*).",
            ),
        ] = None,
        dest: Annotated[
            Path | None,
            typer.Option("--dest", help="Local destination directory (default: BASE_DIR).", file_okay=False),
        ] = None,
        engine: Annotated[
            list[str],
            typer.Option("--engine", "-e", help="Only pull these engine folders (repeatable)."),
        ] = [],
        database: Annotated[
            list[str],
            typer.Option("--db", "-d", help="Only pull these database folders (repeatable)."),
        ] = [],
        include: Annotated[
            list[str],
            typer.Option("--include", "-i", help="rclone glob to include (repeatable), e.g. *.dump.gz."),
        ] = [],
        exclude: Annotated[
            list[str],
            typer.Option(
                "--exclude", "-x",
                help="Glob of files to never download (repeatable), e.g. '*_inc.*', 'manifest_*.txt' or "
                     "'postgres/sonar/*'. Also PULL_EXCLUDE in config (| separated).",
            ),
        ] = [],
        since: Annotated[
            int | None,
            typer.Option("--since", help="Only pull files modified in the last N days."),
        ] = None,
        parallel: Annotated[
            int,
            typer.Option("--parallel", "-j", help="Engine/database folders to download at the same time."),
        ] = 4,
        transfers: Annotated[
            int,
            typer.Option("--transfers", help="Parallel file transfers inside each folder job."),
        ] = 4,
        checkers: Annotated[
            int,
            typer.Option("--checkers", help="Parallel checkers inside each folder job."),
        ] = 8,
        bwlimit: Annotated[
            str | None,
            typer.Option("--bwlimit", help="Bandwidth limit per job, e.g. 10M (rclone syntax)."),
        ] = None,
        overwrite: Annotated[
            bool,
            typer.Option("--overwrite", help="Download everything again, replacing local files (ignores history)."),
        ] = False,
        ignore_history: Annotated[
            bool,
            typer.Option(
                "--ignore-history",
                help="Also download files that were pulled before but are no longer in the local folder.",
            ),
        ] = False,
        delete_remote: Annotated[
            bool | None,
            typer.Option(
                "--delete-remote/--no-delete-remote",
                help="After pulling, delete the files on the server that are verified to be in the local folder "
                     "with the same size (default: PULL_DELETE_REMOTE, false). Needs --yes.",
            ),
        ] = None,
        keep_remote: Annotated[
            int | None,
            typer.Option(
                "--keep-remote",
                help="With --delete-remote: newest backups per database to leave on the server (default: "
                     "PULL_KEEP_REMOTE, 0).",
            ),
        ] = None,
        verify: Annotated[
            bool,
            typer.Option("--verify", help="With --delete-remote: also compare checksums (rclone check) first."),
        ] = False,
        yes: Annotated[
            bool,
            typer.Option("--yes", "-y", help="Confirm deleting server files without asking."),
        ] = False,
        no_manifest: Annotated[
            bool,
            typer.Option(
                "--no-manifest",
                help="Do not read manifest.json from the server; list its folders instead (slower).",
            ),
        ] = False,
        dry_run: Annotated[
            bool,
            typer.Option("--dry-run", "-n", help="Show what would be downloaded; change nothing."),
        ] = False,
        rclone: Annotated[str | None, _RCLONE_OPT] = None,
        verbose: Annotated[int, _VERBOSE_OPT] = 0,
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Download backups from an rclone remote to local storage, in parallel.

    Expects the layout `sync` uploads: <remote>/<engine>/<database>/<files>. Each
    engine/database folder is its own rclone job and --parallel jobs run at once.
    Never deletes anything, and skips files that already exist locally unless
    --overwrite is given.

    Use --remote for any rclone remote you have configured (rclone config), or set
    PULL_REMOTE so a plain `pull` works.
    """
    from .pull import run_pull
    from .storage import set_verbose, use_rclone

    _enable_json(as_json)
    for e in engine:
        _validate_db_type(e)
    cfg = load_config(config_file)
    if rclone:
        use_rclone(rclone)
    set_verbose(verbose)
    deleting = cfg.pull_delete_remote if delete_remote is None else delete_remote
    if deleting and not dry_run and not yes and not json_enabled() and sys.stdin.isatty():
        from . import prompt as q

        console.print(
            "[bold yellow]--delete-remote removes files on the server[/] once they are verified in the local folder."
        )
        yes = bool(q.confirm("Delete the pulled files on the server?", default=False).ask())
        if not yes:
            console.print("[yellow]Aborted.[/]")
            raise typer.Exit(code=EXIT_ABORTED)
    code = run_pull(
        cfg,
        remote=remote or None,
        dest=dest,
        engines=list(engine) or None,
        databases=list(database) or None,
        include_patterns=list(include) or None,
        since=since,
        workers=parallel,
        transfers=transfers,
        checkers=checkers,
        bwlimit=bwlimit,
        dry_run=dry_run,
        overwrite=overwrite,
        ignore_history=ignore_history,
        delete_remote=delete_remote,
        keep_remote=keep_remote,
        verify=verify,
        yes=yes,
        use_manifest=not no_manifest,
        exclude_patterns=list(exclude) or None,
    )
    if code:
        raise typer.Exit(code=code)


@app.command("sync")
def sync_cmd(
        remote: Annotated[
            list[str],
            typer.Option(
                "--remote", "-r",
                help="Destination: an rclone remote (s3:bucket/backups, b2:bucket, ...) or a local folder "
                     "(D:/OneDrive/Backups). Repeat for several destinations; default PUSH_REMOTE.",
            ),
        ] = [],
        dry_run: Annotated[
            bool,
            typer.Option("--dry-run", "-d", help="Show what would happen — no files moved or deleted."),
        ] = False,
        days: Annotated[
            int | None,
            typer.Option("--days", "-n", help="Prune remote files older than N days (overrides BACKUP_AGE)."),
        ] = None,
        include: Annotated[
            list[str],
            typer.Option("--include", "-i", help="Glob pattern to include (repeatable, overrides INCLUDE_FILES)."),
        ] = [],
        keep_local: Annotated[
            bool | None,
            typer.Option(
                "--keep-local/--delete-local",
                help="Keep or delete the local files after a successful upload (default: PUSH_DELETE_LOCAL, true). "
                     "Keep them if you also `pull`, or the next pull downloads them again.",
            ),
        ] = None,
        list_remotes: Annotated[
            bool,
            typer.Option("--list-remotes", help="List the rclone remotes configured on this host and exit."),
        ] = False,
        no_interactive: Annotated[
            bool,
            typer.Option("--no-interactive", "-ni", help="Skip interactive selection of schemas and date range."),
        ] = False,
        transfers: Annotated[
            int | None,
            typer.Option("--transfers", help="Number of parallel file transfers. Default: 4."),
        ] = None,
        checkers: Annotated[
            int | None,
            typer.Option("--checkers", help="Number of parallel checkers. Default: 8."),
        ] = None,
        since: Annotated[
            int | None,
            typer.Option("--since", help="Only upload files modified in the last N days."),
        ] = None,
        until: Annotated[
            int | None,
            typer.Option("--until", help="Only upload files modified before N days ago."),
        ] = None,
        schema: Annotated[
            list[str],
            typer.Option("--schema", "-s", help="Only upload artifacts for these databases/schemas (repeatable)."),
        ] = [],
        latest: Annotated[
            int | None,
            typer.Option(
                "--latest", "-L",
                help="Upload only the newest N backups of each database (default: PUSH_LATEST, 3). 0 = all files.",
            ),
        ] = None,
        quota: Annotated[
            str | None,
            typer.Option(
                "--quota", "-q",
                help="Remote size limit, e.g. 50G (default: PUSH_QUOTA). When set, the oldest remote backups are "
                     "pruned to fit it after the upload, and the 2-day age prune only runs if --days is given.",
            ),
        ] = None,
        rclone: Annotated[str | None, _RCLONE_OPT] = None,
        verbose: Annotated[int, _VERBOSE_OPT] = 0,
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Upload backups to any rclone remote and prune old files there.

    Also available as `push`. Set PUSH_REMOTE in config to use a configured rclone
    remote by default.

    Works with every backend rclone supports: Google Drive, S3, S3-compatible
    (MinIO, Wasabi), Backblaze B2, SFTP, Azure Blob, WebDAV. Local files are
    deleted only after a successful upload.
    """
    from .storage import run_sync, set_verbose, use_rclone

    _enable_json(as_json)
    cfg = load_config(config_file)
    if rclone:
        use_rclone(rclone)
    set_verbose(verbose)
    code = run_sync(
        cfg,
        remote=list(remote) or None,
        dry_run=dry_run,
        age_days=days,
        include_patterns=list(include) or None,
        delete_local=cfg.push_delete_local if keep_local is None else not keep_local,
        show_remotes=list_remotes,
        transfers=transfers,
        checkers=checkers,
        since=since,
        until=until,
        schemas=list(schema),
        interactive=not no_interactive,
        latest=latest,
        quota=quota,
    )
    if code:
        raise typer.Exit(code=code)


app.command("push", help="Upload backups to an rclone remote (alias of sync).")(sync_cmd)


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
        as_json: Annotated[bool, _JSON_OPT] = False,
        config_file: Annotated[Path | None, _CONFIG_OPT] = None,
) -> None:
    """Migrate one or more databases from MariaDB to PostgreSQL.

    Requires both MY_* (source) and PG_* (target) credentials in .backup config.
    Use --dry-run to preview the migration plan without writing any data.
    """
    from .config import DbType
    from .migrate import run_migrate

    _enable_json(as_json)
    all_cfgs = load_all_configs(config_file)
    src_cfgs = [c for c in all_cfgs if c.db_type == DbType.MARIADB]
    dst_cfgs = [c for c in all_cfgs if c.db_type == DbType.POSTGRES]

    if not src_cfgs:
        _fail(
            "No MariaDB source config found. "
            "Set MY_USERNAME and MY_HOST in .backup (multi-engine mode).",
            EXIT_FAILURE,
        )
    if not dst_cfgs:
        _fail(
            "No PostgreSQL target config found. "
            "Set PG_USERNAME and PG_HOST in .backup (multi-engine mode).",
            EXIT_FAILURE,
        )

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
