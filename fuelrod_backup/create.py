"""Create wizard: interactively create a database or a PostgreSQL schema."""

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
    console.rule(f"[bold green]{title}[/]")
    console.print()


def _die(msg: str) -> None:
    console.print(f"[bold red]ERROR:[/] {msg}")
    sys.exit(1)


def run_create(cfg: Config) -> None:
    """Interactive wizard to create a database or schema."""
    adapter = get_adapter(cfg)

    if not cfg.password:
        _die("Password is required. Set the appropriate *_PASSWORD variable in .backup.")

    console.print()
    console.print(Panel(
        f"[bold green]CREATE WIZARD — {cfg.db_type.value.upper()}[/]",
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

    # ── Choose target type ─────────────────────────────────────────
    _section("Create Target")

    choices = [questionary.Choice("Database", value="database")]
    if adapter.supports_schema_create:
        choices.append(questionary.Choice(
            "Schema    (CREATE SCHEMA inside an existing database)",
            value="schema",
        ))

    if len(choices) == 1:
        target_type = "database"
        console.print("[dim]Only 'database' creation is supported for this engine.[/]")
    else:
        target_type = questionary.select("What do you want to create?", choices=choices).ask()

    if target_type == "schema":
        _create_schema(cfg, adapter)
    else:
        _create_database(cfg, adapter)


# ──────────────────────────────────────────────────────────────────────────────
#  Database creation
# ──────────────────────────────────────────────────────────────────────────────

def _create_database(cfg: Config, adapter: DbAdapter) -> None:
    _section("Select Database Name")

    console.print("  Existing databases on this server:")
    all_dbs = adapter.list_databases()
    if all_dbs:
        tbl = Table(show_header=True, header_style="bold")
        tbl.add_column("#", style="dim", width=4)
        tbl.add_column("Database", min_width=28)
        tbl.add_column("Size", justify="right")
        for i, db in enumerate(all_dbs, 1):
            tbl.add_row(str(i), db, adapter.get_db_size(db))
        console.print(tbl)
    else:
        console.print("  [dim](no user databases yet)[/]")

    console.print()
    db_name = questionary.text("Enter name for the new database:").ask() or ""
    db_name = db_name.strip()
    if not db_name:
        _die("Database name cannot be empty.")

    # Check if it already exists
    if adapter.db_exists(db_name):
        _die(f"Database '{db_name}' already exists.")

    # Confirm
    console.print()
    console.print(f"  [bold]Database:[/] [cyan]{db_name}[/] will be created.")
    if not questionary.confirm("Create this database?", default=True).ask():
        console.print("[yellow]Aborted.[/]")
        sys.exit(0)

    # Execute
    console.print()
    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}", table_column=Column(min_width=48)),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"[cyan]Creating database '{db_name}'…[/]", total=None)
        try:
            adapter.create_db(db_name)
            progress.update(
                task,
                description=f"[green]✓ Database '{db_name}' created[/]",
                total=1, completed=1,
            )
        except Exception as exc:
            progress.update(task, description="[red]✗ Create failed[/]", total=1, completed=1)
            _die(f"Create failed: {exc}")

    console.print()
    console.print(Panel(f"[bold green]DATABASE '{db_name}' CREATED[/]", expand=False))


# ──────────────────────────────────────────────────────────────────────────────
#  Schema creation (PostgreSQL only)
# ──────────────────────────────────────────────────────────────────────────────

def _create_schema(cfg: Config, adapter: DbAdapter) -> None:
    # Pick the target database
    _section("Select Database")

    all_dbs = adapter.list_databases()
    if not all_dbs:
        _die("No databases found on server.")

    db_name = questionary.select(
        "Select database to create the schema in",
        choices=[questionary.Choice(db, value=db) for db in all_dbs],
    ).ask()

    # List existing schemas in that database
    _section("Schema Name")

    schemas = adapter.get_user_schemas(db_name)
    if schemas:
        console.print("  Existing schemas in this database:")
        tbl = Table(show_header=True, header_style="bold")
        tbl.add_column("#", style="dim", width=4)
        tbl.add_column("Schema", min_width=28)
        for i, s in enumerate(schemas, 1):
            tbl.add_row(str(i), s)
        console.print(tbl)

    console.print()
    schema_name = questionary.text("Enter name for the new schema:").ask() or ""
    schema_name = schema_name.strip()
    if not schema_name:
        _die("Schema name cannot be empty.")

    if schema_name in schemas:
        _die(f"Schema '{schema_name}' already exists in '{db_name}'.")

    # Confirm
    console.print()
    console.print(f"  [bold]Schema:[/] [cyan]{schema_name}[/] in database [cyan]{db_name}[/] will be created.")
    if not questionary.confirm("Create this schema?", default=True).ask():
        console.print("[yellow]Aborted.[/]")
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
            f"[cyan]Creating schema '{schema_name}' in '{db_name}'…[/]",
            total=None,
        )
        try:
            adapter.create_schema(db_name, schema_name)
            progress.update(
                task,
                description=f"[green]✓ Schema '{schema_name}' created in '{db_name}'[/]",
                total=1, completed=1,
            )
        except Exception as exc:
            progress.update(task, description="[red]✗ Create failed[/]", total=1, completed=1)
            _die(f"Create failed: {exc}")

    console.print()
    console.print(Panel(
        f"[bold green]SCHEMA '{schema_name}' CREATED IN '{db_name}'[/]",
        expand=False,
    ))
