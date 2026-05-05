"""Interactive migration wizard (follows backup.py _wizard_* style)."""

from __future__ import annotations

import sys

from rich.console import Console
from rich.panel import Panel

from .. import prompt as questionary
from ..adapters.mariadb import MariaDbAdapter

console = Console()


def _section(title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{title}[/]")
    console.print()


def _die(msg: str) -> None:
    console.print(f"[bold red]ERROR:[/] {msg}")
    sys.exit(1)


def run_migrate_wizard(src_cfg, dst_cfg) -> tuple:
    """Interactive wizard — returns (src_db, dst_db, options_dict) or exits."""
    console.print(Panel(
        "[bold cyan]MariaDB → PostgreSQL Migration Wizard[/]",
        expand=False,
    ))

    # ── Source connection ─────────────────────────────────────────────────────
    _section("Source Connection (MariaDB)")
    src_adapter = MariaDbAdapter(src_cfg)

    console.print(f"  Engine: [cyan]mariadb[/]")
    if src_cfg.use_docker:
        console.print(f"  Mode  : [cyan]Docker[/] — service '[bold]{src_cfg.service}[/]'")
    else:
        console.print(f"  Mode  : Direct — {src_cfg.host}:{src_cfg.port}")
    console.print(f"  User  : {src_cfg.user}")
    console.print()

    if questionary.confirm("Override source connection settings?", default=False).ask():
        if not src_cfg.use_docker:
            src_cfg.host = questionary.text("Source host", default=src_cfg.host).ask() or src_cfg.host
            src_cfg.port = int(questionary.text("Source port", default=str(src_cfg.port)).ask() or src_cfg.port)
        src_cfg.user = questionary.text("Source username", default=src_cfg.user).ask() or src_cfg.user
        new_pass = questionary.password("Source password (blank = keep current)").ask() or ""
        if new_pass:
            src_cfg.password = new_pass

    if not src_cfg.password:
        _die("Source password is required. Set MY_PASSWORD in .backup.")

    try:
        questionary.check_connection_with_countdown(src_adapter.check_connection, src_cfg.connection_timeout)
    except TimeoutError as exc:
        _die(str(exc))
    console.print("[green]Source connection OK.[/]")

    # ── Target connection ─────────────────────────────────────────────────────
    _section("Target Connection (PostgreSQL)")
    from ..adapters.postgres import PostgresAdapter
    dst_adapter = PostgresAdapter(dst_cfg)

    console.print(f"  Engine: [cyan]postgres[/]")
    if dst_cfg.use_docker:
        console.print(f"  Mode  : [cyan]Docker[/] — service '[bold]{dst_cfg.service}[/]'")
    else:
        console.print(f"  Mode  : Direct — {dst_cfg.host}:{dst_cfg.port}")
    console.print(f"  User  : {dst_cfg.user}")
    console.print()

    if questionary.confirm("Override target connection settings?", default=False).ask():
        if not dst_cfg.use_docker:
            dst_cfg.host = questionary.text("Target host", default=dst_cfg.host).ask() or dst_cfg.host
            dst_cfg.port = int(questionary.text("Target port", default=str(dst_cfg.port)).ask() or dst_cfg.port)
        dst_cfg.user = questionary.text("Target username", default=dst_cfg.user).ask() or dst_cfg.user
        new_pass = questionary.password("Target password (blank = keep current)").ask() or ""
        if new_pass:
            dst_cfg.password = new_pass

    if not dst_cfg.password:
        _die("Target password is required. Set PG_PASSWORD in .backup.")

    try:
        questionary.check_connection_with_countdown(dst_adapter.check_connection, dst_cfg.connection_timeout)
    except TimeoutError as exc:
        _die(str(exc))
    console.print("[green]Target connection OK.[/]")

    # ── Select source database ────────────────────────────────────────────────
    _section("Select Source Database")
    all_dbs = src_adapter.list_databases()
    if not all_dbs:
        _die("No databases found on MariaDB server.")

    src_db: str = questionary.select(
        "Source database to migrate",
        choices=all_dbs,
    ).ask()

    # ── Target database name ──────────────────────────────────────────────────
    _section("Target Database")
    dst_db_input = questionary.text(
        "Target PostgreSQL database name",
        default=src_db,
    ).ask() or src_db
    dst_db = dst_db_input

    # ── Target schema ─────────────────────────────────────────────────────────
    target_schema: str = questionary.text(
        "Target PostgreSQL schema",
        default="public",
    ).ask() or "public"

    # ── Migration options ─────────────────────────────────────────────────────
    _section("Migration Options")

    batch_size = int(
        questionary.text("Batch size (rows per INSERT)", default="1000").ask() or "1000"
    )
    parallel = int(
        questionary.text("Parallel workers (tables)", default="4").ask() or "4"
    )
    validate = questionary.confirm("Validate row counts after migration?", default=True).ask()
    validate_checksums = False
    if validate:
        validate_checksums = questionary.confirm(
            "Also compare MD5 checksums? (slower)", default=False
        ).ask()

    # ── ENUM handling ─────────────────────────────────────────────────────────
    _section("ENUM Handling")
    enum_choice = questionary.select(
        "How to handle ENUM columns?",
        choices=[
            questionary.Choice("TEXT + CHECK constraint (safe, no global type)", value="check"),
            questionary.Choice("CREATE TYPE enum (named PG enum type)", value="type"),
        ],
        default="check",
    ).ask()
    enum_as_type = enum_choice == "type"

    # ── Summary ───────────────────────────────────────────────────────────────
    _section("Migration Summary")
    console.print(f"  Source     : mariadb://{src_cfg.user}@{src_cfg.host}:{src_cfg.port}/{src_db}")
    console.print(f"  Target     : postgres://{dst_cfg.user}@{dst_cfg.host}:{dst_cfg.port}/{dst_db}")
    console.print(f"  Schema     : {target_schema}")
    console.print(f"  Batch size : {batch_size}")
    console.print(f"  Workers    : {parallel}")
    console.print(f"  Validate   : {validate} (checksums: {validate_checksums})")
    console.print(f"  ENUM mode  : {'CREATE TYPE' if enum_as_type else 'TEXT + CHECK'}")
    console.print()

    if not questionary.confirm(
        "[bold red]Proceed? This will write data to the target PostgreSQL database.[/]",
        default=False,
    ).ask():
        console.print("[yellow]Aborted.[/]")
        sys.exit(0)

    options = dict(
        target_schema=target_schema,
        batch_size=batch_size,
        parallel=parallel,
        validate=validate,
        validate_checksums=validate_checksums,
        enum_as_type=enum_as_type,
    )
    return src_db, dst_db, options
