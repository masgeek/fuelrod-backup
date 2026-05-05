"""Drop wizard: interactively drop a database or a PostgreSQL schema."""

from __future__ import annotations

import sys

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Column, Table

from . import prompt as questionary
from .adapters import get_adapter
from .adapters.base import DbAdapter
from .config import Config

console = Console()


def _section(title: str) -> None:
    console.print()
    console.rule(f"[bold red]{title}[/]")
    console.print()


def _die(msg: str) -> None:
    console.print(f"[bold red]ERROR:[/] {msg}")
    sys.exit(1)


def run_drop(cfg: Config) -> None:
    """Interactive wizard to drop a database or schema."""
    adapter = get_adapter(cfg)

    if not cfg.password:
        _die("Password is required. Set the appropriate *_PASSWORD variable in .backup.")

    console.print()
    console.print(Panel(
        f"[bold red]⚠  DROP WIZARD — {cfg.db_type.value.upper()}  ⚠[/]\n"
        "[dim]This operation is irreversible. All dropped data is permanently gone.[/]",
        expand=False,
    ))

    # ── Connection ─────────────────────────────────────────────────
    _section("Connection")
    console.print(f"  Engine: [cyan]{cfg.db_type.value}[/]")
    if cfg.use_docker:
        console.print(f"  Mode  : [cyan]Docker[/] — service '[bold]{cfg.service}[/]'")
    else:
        console.print(f"  Mode  : Direct — {cfg.host}:{cfg.port}")
    console.print(f"  User  : {cfg.user}")
    console.print()
    try:
        questionary.check_connection_with_countdown(adapter.check_connection, cfg.connection_timeout)
    except TimeoutError as exc:
        _die(str(exc))
    console.print("[green]Connection OK.[/]")

    # ── Choose target type ──────────────────────────────────────────
    _section("Drop Target")

    choices = [questionary.Choice("Database  (kills all connections, then drops)", value="database")]
    if adapter.supports_schema_drop:
        choices.append(questionary.Choice(
            "Schema    (DROP SCHEMA … CASCADE — removes all objects inside)",
            value="schema",
        ))

    if len(choices) == 1:
        target_type = "database"
        console.print("[dim]Only 'database' drop is supported for this engine.[/]")
    else:
        target_type = questionary.select("What do you want to drop?", choices=choices).ask()

    if target_type == "schema":
        _drop_schema(cfg, adapter)
    else:
        _drop_database(cfg, adapter)


# ──────────────────────────────────────────────────────────────────────────────
#  Database drop
# ──────────────────────────────────────────────────────────────────────────────

def _drop_database(cfg: Config, adapter: DbAdapter) -> None:
    _section("Select Database")

    all_dbs = adapter.list_databases()
    if not all_dbs:
        _die("No databases found on server.")

    tbl = Table(show_header=True, header_style="bold")
    tbl.add_column("#", style="dim", width=4)
    tbl.add_column("Database", min_width=28)
    tbl.add_column("Size", justify="right")
    for i, db in enumerate(all_dbs, 1):
        tbl.add_row(str(i), db, adapter.get_db_size(db))
    console.print(tbl)

    db_name: str = questionary.select(
        "Select database to drop",
        choices=[questionary.Choice(db, value=db) for db in all_dbs],
    ).ask()

    # Confirm
    console.print()
    console.print(
        f"  [bold red]WARNING:[/] [bold]{db_name}[/] and ALL its data will be permanently deleted."
    )
    console.print("  All active connections will be terminated first.")
    console.print()
    typed = questionary.text(f'  Type "[bold]{db_name}[/]" to confirm:').ask() or ""
    if typed.strip() != db_name:
        console.print("[yellow]Name did not match — aborted.[/]")
        sys.exit(0)

    # Execute
    console.print()
    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}", table_column=Column(min_width=48)),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"[cyan]Killing connections to '{db_name}'…[/]", total=None)
        try:
            killed = adapter.terminate_connections(db_name)
            desc = (
                f"[cyan]Killed {killed} connection(s) — dropping '{db_name}'…[/]"
                if killed else
                f"[cyan]Dropping '{db_name}'…[/]"
            )
            progress.update(task, description=desc)
            # drop_db on PgRunner re-calls terminate_connections (harmless, returns 0),
            # then issues DROP DATABASE. Other adapters handle it themselves.
            adapter.drop_db(db_name)
            progress.update(
                task,
                description=f"[green]✓ Database '{db_name}' dropped[/]",
                total=1, completed=1,
            )
        except Exception as exc:
            progress.update(task, description="[red]✗ Drop failed[/]", total=1, completed=1)
            _die(f"Drop failed: {exc}")

    console.print()
    console.print(Panel(f"[bold green]DATABASE '{db_name}' DROPPED[/]", expand=False))


# ──────────────────────────────────────────────────────────────────────────────
#  Schema drop (PostgreSQL only)
# ──────────────────────────────────────────────────────────────────────────────

def _drop_schema(cfg: Config, adapter: DbAdapter) -> None:
    # Pick the database that contains the schema
    _section("Select Database")

    all_dbs = adapter.list_databases()
    if not all_dbs:
        _die("No databases found on server.")

    db_name: str = questionary.select(
        "Select database containing the schema",
        choices=[questionary.Choice(db, value=db) for db in all_dbs],
    ).ask()

    # List schemas inside that database
    _section("Select Schema")

    schemas = adapter.get_user_schemas(db_name)
    if not schemas:
        _die(f"No user-defined schemas found in '{db_name}'.")

    tbl = Table(show_header=True, header_style="bold")
    tbl.add_column("#", style="dim", width=4)
    tbl.add_column("Schema", min_width=28)
    for i, s in enumerate(schemas, 1):
        tbl.add_row(str(i), s)
    console.print(tbl)

    schema_name: str = questionary.select(
        "Select schema to drop (CASCADE — tables, views, functions, sequences all deleted)",
        choices=[questionary.Choice(s, value=s) for s in schemas],
    ).ask()

    # Confirm
    console.print()
    console.print(
        f"  [bold red]WARNING:[/] Schema [bold]{schema_name}[/] in database [bold]{db_name}[/] "
        "will be permanently deleted."
    )
    console.print("  Every table, view, function and sequence inside it will be gone.")
    console.print()
    typed = questionary.text(f'  Type "[bold]{schema_name}[/]" to confirm:').ask() or ""
    if typed.strip() != schema_name:
        console.print("[yellow]Name did not match — aborted.[/]")
        sys.exit(0)

    # Execute
    console.print()
    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}", table_column=Column(min_width=48)),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"[cyan]Dropping schema '{schema_name}' in '{db_name}' (CASCADE)…[/]",
            total=None,
        )
        try:
            adapter.drop_schema(db_name, schema_name)
            progress.update(
                task,
                description=f"[green]✓ Schema '{schema_name}' dropped from '{db_name}'[/]",
                total=1, completed=1,
            )
        except Exception as exc:
            progress.update(task, description="[red]✗ Drop failed[/]", total=1, completed=1)
            _die(f"Drop failed: {exc}")

    console.print()
    console.print(Panel(
        f"[bold green]SCHEMA '{schema_name}' DROPPED FROM '{db_name}'[/]",
        expand=False,
    ))
