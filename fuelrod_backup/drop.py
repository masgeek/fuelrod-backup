"""Drop wizard: drop a database/schema, or disconnect clients from a database."""

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
from .notify import EXIT_ABORTED, EXIT_FAILURE
from .output import emit, error, json_enabled, register

console = register(Console())


def _section(title: str) -> None:
    console.print()
    console.rule(f"[bold red]{title}[/]")
    console.print()


def _die(msg: str) -> None:
    if json_enabled():
        error(msg)
        sys.exit(EXIT_FAILURE)
    console.print(f"[bold red]ERROR:[/] {msg}")
    sys.exit(1)


def _fmt(val: str) -> str:
    """Return val or dimmed '?' if unknown."""
    return val if val and val != "?" else "[dim]?[/]"


def run_drop(
    cfg: Config,
    *,
    disconnect_only: bool = False,
    db_name: str | None = None,
) -> None:
    """Interactive wizard to drop a database or schema.

    With ``disconnect_only=True`` the wizard skips the destructive path and
    only terminates active connections to ``db_name`` (or the selected one).
    """
    adapter = get_adapter(cfg)

    if not cfg.password:
        _die("Password is required. Set the appropriate *_PASSWORD variable in .backup.")

    if disconnect_only:
        _disconnect_database(cfg, adapter, db_name)
        return

    console.print()
    console.print(Panel(
        f"[bold red]⚠  DROP WIZARD — {cfg.db_type.value.upper()}  ⚠[/]\n"
        "[dim]This operation is irreversible. All dropped data is permanently gone.[/]",
        expand=False,
    ))

    _check_connection(cfg, adapter)

    # ── Choose target type ──────────────────────────────────────────
    _section("Drop Target")

    choices = [
        questionary.Choice("Database  (kills all connections, then drops)", value="database"),
        questionary.Choice(
            "Connections  (disconnect every client — nothing is deleted)",
            value="connections",
        ),
    ]
    if adapter.supports_schema_drop:
        choices.append(questionary.Choice(
            "Schema    (DROP SCHEMA … CASCADE — removes all objects inside)",
            value="schema",
        ))

    target_type = questionary.select("What do you want to drop?", choices=choices).ask()

    if target_type == "connections":
        _disconnect_database(cfg, adapter, db_name)
    elif target_type == "schema":
        _drop_schema(cfg, adapter)
    else:
        _drop_database(cfg, adapter)


def _check_connection(cfg: Config, adapter: DbAdapter) -> None:
    """Print the resolved connection and verify it, or exit."""
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


# ──────────────────────────────────────────────────────────────────────────────
#  Disconnect only (no drop)
# ──────────────────────────────────────────────────────────────────────────────

def _disconnect_database(cfg: Config, adapter: DbAdapter, db_name: str | None = None) -> None:
    """Terminate all connections to a database without deleting anything."""
    console.print()
    console.print(Panel(
        f"[bold yellow]DISCONNECT — {cfg.db_type.value.upper()}[/]\n"
        "[dim]Kills every active session on the database. No data is deleted.[/]",
        expand=False,
    ))

    _check_connection(cfg, adapter)

    all_dbs = adapter.list_databases()
    if not all_dbs:
        _die("No databases found on server.")

    if db_name:
        if db_name not in all_dbs:
            _die(f"Database '{db_name}' not found on server.")
        target = db_name
    else:
        _section("Select Database")
        target = questionary.select(
            "Select database to disconnect from",
            choices=[questionary.Choice(db, value=db) for db in all_dbs],
        ).ask()

    console.print()
    console.print("  [bold yellow]WARNING:[/] You are about to terminate every connection to:")
    console.print(f"    Database : [bold]{target}[/]")
    console.print(f"    Tables   : {_fmt(adapter.get_table_count(target))}")
    console.print()
    console.print("  [yellow]Active transactions will be rolled back and clients will lose[/]")
    console.print("  [yellow]their session. Nothing inside the database is deleted.[/]")
    console.print()
    typed = questionary.text(f'  Type "[bold]{target}[/]" to confirm:').ask() or ""
    if typed.strip() != target:
        console.print("[yellow]Name did not match — aborted.[/]")
        sys.exit(EXIT_ABORTED)

    console.print()
    try:
        killed = adapter.terminate_connections(target)
    except Exception as exc:
        _die(f"Disconnect failed: {exc}")

    if json_enabled():
        emit({
            "command": "drop",
            "action": "disconnect",
            "engine": cfg.db_type.value,
            "database": target,
            "connections_terminated": killed,
            "dropped": False,
        })
        return

    console.print()
    if killed:
        console.print(Panel(
            f"[bold green]{killed} CONNECTION(S) TERMINATED[/] on '{target}'\n"
            "[dim]No data was deleted.[/]",
            expand=False,
        ))
    else:
        console.print(Panel(
            f"[bold yellow]NO ACTIVE CONNECTIONS[/] on '{target}'\n"
            "[dim]No data was deleted.[/]",
            expand=False,
        ))


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
    tbl.add_column("Database", min_width=24)
    tbl.add_column("Tables", justify="right")
    tbl.add_column("Size", justify="right")
    for i, db in enumerate(all_dbs, 1):
        tbl.add_row(
            str(i),
            db,
            _fmt(adapter.get_table_count(db)),
            _fmt(adapter.get_db_size(db)),
        )
    console.print(tbl)

    db_name: str = questionary.select(
        "Select database to drop",
        choices=[questionary.Choice(db, value=db) for db in all_dbs],
    ).ask()

    # Gather summary details
    table_count = adapter.get_table_count(db_name)
    db_size = adapter.get_db_size(db_name)
    schemas = adapter.get_user_schemas(db_name)

    # Confirm
    console.print()
    summary_parts = [
        "[bold red]WARNING:[/] You are about to permanently delete:",
        f"  [bold]{db_name}[/]",
        f"  Tables : {_fmt(table_count)}",
        f"  Size   : {_fmt(db_size)}",
    ]
    if schemas:
        summary_parts.append(f"  Schemas: {len(schemas)}")
    summary_parts.append("")
    summary_parts.append("  All active connections will be terminated first.")
    summary_parts.append("  [bold]This operation cannot be undone.[/]")
    console.print("\n".join(summary_parts))
    console.print()
    typed = questionary.text(f'  Type "[bold]{db_name}[/]" to confirm:').ask() or ""
    if typed.strip() != db_name:
        console.print("[yellow]Name did not match — aborted.[/]")
        sys.exit(EXIT_ABORTED)

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
    tbl.add_column("Schema", min_width=24)
    tbl.add_column("Tables", justify="right")
    for i, s in enumerate(schemas, 1):
        tbl.add_row(
            str(i),
            s,
            _fmt(adapter.get_table_count(db_name, s)),
        )
    console.print(tbl)

    schema_name: str = questionary.select(
        "Select schema to drop (CASCADE — tables, views, functions, sequences all deleted)",
        choices=[questionary.Choice(s, value=s) for s in schemas],
    ).ask()

    # Gather summary
    table_count = adapter.get_table_count(db_name, schema_name)

    # Confirm
    console.print()
    console.print(
        "  [bold red]WARNING:[/] You are about to permanently delete:"
    )
    console.print(f"    Schema   : [bold]{schema_name}[/]")
    console.print(f"    Database : [bold]{db_name}[/]")
    console.print(f"    Tables   : {_fmt(table_count)}")
    console.print()
    console.print("  Every table, view, function and sequence inside it will be gone.")
    console.print("  [bold]This operation cannot be undone.[/]")
    console.print()
    typed = questionary.text(f'  Type "[bold]{schema_name}[/]" to confirm:').ask() or ""
    if typed.strip() != schema_name:
        console.print("[yellow]Name did not match — aborted.[/]")
        sys.exit(EXIT_ABORTED)

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
