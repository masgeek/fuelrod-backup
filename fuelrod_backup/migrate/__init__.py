"""MariaDB → PostgreSQL migration entry point."""

from __future__ import annotations

import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from ..output import emit, error, json_enabled, register
from .report import MigrationReport
from .runner import MigrateError, MigrateRunner

console = register(Console())

__all__ = ["MigrateError", "run_migrate"]


def run_migrate(
    src_cfg,
    dst_cfg,
    *,
    interactive: bool = True,
    source_db: str | None = None,
    target_db: str | None = None,
    target_schema: str = "public",
    batch_size: int = 1000,
    parallel: int = 4,
    dry_run: bool = False,
    validate: bool = True,
    validate_checksums: bool = False,
    fail_fast: bool = False,
    unsigned_checks: bool = False,
    enum_as_type: bool = False,
    skip_tables: list[str] | None = None,
    only_tables: list[str] | None = None,
    report_file: Path | None = None,
) -> None:
    if interactive:
        from .wizard import run_migrate_wizard
        src_db, dst_db, opts = run_migrate_wizard(src_cfg, dst_cfg)
        target_schema = opts.get("target_schema", target_schema)
        batch_size = opts.get("batch_size", batch_size)
        parallel = opts.get("parallel", parallel)
        validate = opts.get("validate", validate)
        validate_checksums = opts.get("validate_checksums", validate_checksums)
        enum_as_type = opts.get("enum_as_type", enum_as_type)
    else:
        if not source_db:
            if json_enabled():
                error("--source-db is required in non-interactive mode.")
            else:
                console.print("[bold red]ERROR:[/] --source-db is required in non-interactive mode.")
            sys.exit(1)
        src_db = source_db
        dst_db = target_db or source_db

    runner = MigrateRunner(
        src_cfg=src_cfg,
        dst_cfg=dst_cfg,
        target_schema=target_schema,
        batch_size=batch_size,
        parallel=parallel,
        dry_run=dry_run,
        validate=validate,
        validate_checksums=validate_checksums,
        fail_fast=fail_fast,
        unsigned_checks=unsigned_checks,
        enum_as_type=enum_as_type,
        skip_tables=skip_tables,
        only_tables=only_tables,
        report_file=report_file,
    )

    try:
        report: MigrationReport = runner.migrate_database(src_db, dst_db)
    except MigrateError as exc:
        if json_enabled():
            error(str(exc))
        else:
            console.print(f"\n[bold red]Migration failed:[/] {exc}")
        sys.exit(1)

    if json_enabled():
        emit({
            "command": "migrate",
            "status": "fail" if report.has_failures() else "ok",
            "source": {"engine": src_cfg.db_type.value, "database": src_db},
            "target": {
                "engine": dst_cfg.db_type.value,
                "database": dst_db,
                "schema": target_schema,
            },
            "dry_run": dry_run,
            "report_file": str(report_file) if report_file else None,
            **report.to_dict(),
        })
        if report.has_failures():
            sys.exit(1)
        return

    console.print()
    if dry_run:
        console.print(Panel("[bold yellow]DRY RUN COMPLETE — no data written[/]", expand=False))
        return

    report.print_summary(console)
    console.print()

    if report.has_failures():
        console.print(Panel("[bold red]MIGRATION COMPLETE WITH ERRORS[/]", expand=False))
        sys.exit(1)
    else:
        console.print(Panel("[bold green]MIGRATION COMPLETE[/]", expand=False))
