"""Google Drive sync via rclone-python (replaces gbk.sh)."""

from __future__ import annotations

import sys
from pathlib import Path

import rclone
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TransferSpeedColumn,
)
from rich.table import Table

from .config import Config

console = Console()


def _section(title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{title}[/]")
    console.print()


def _die(msg: str) -> None:
    console.print(f"[bold red]ERROR:[/] {msg}")
    sys.exit(1)


def _human_size(path: Path) -> str:
    size = path.stat().st_size
    if size >= 1024 * 1024:
        return f"{size / 1024 / 1024:.1f} MB"
    return f"{size / 1024:.1f} KB"


def _collect_files(base_dir: Path, patterns: list[str]) -> list[Path]:
    """Return all files under base_dir matching any of the glob patterns."""
    seen: set[Path] = set()
    result: list[Path] = []
    for pattern in patterns:
        for f in base_dir.rglob(pattern):
            if f.is_file() and f not in seen:
                seen.add(f)
                result.append(f)
    return sorted(result)


def run_gdrive_sync(
        cfg: Config,
        *,
        dry_run: bool = False,
        gdrive_remote: str | None = None,
        age_days: int | None = None,
        include_patterns: list[str] | None = None,
        delete_local: bool = True,
) -> None:
    """
    Sync local backups to Google Drive via rclone-python, then prune old remote files.

    Safety guarantee (fixes gbk.sh risk): local files are only deleted AFTER
    rclone.copy() completes without raising an exception. If the copy fails,
    the exception propagates and no local files are removed.
    """
    if not rclone.is_installed():
        _die("rclone not found in PATH. Install it from https://rclone.org/install/")

    remote = gdrive_remote or cfg.gdrive_remote
    days = age_days if age_days is not None else cfg.gdrive_age_days
    patterns = include_patterns or cfg.gdrive_include
    base_dir = Path(cfg.base_dir)
    remote_path = f"gdrive:{remote}/"

    if not base_dir.is_dir():
        _die(f"Backup directory not found: {base_dir}")

    console.print()
    console.print(Panel(
        f"[bold cyan]Google Drive Sync[/]\n"
        f"  Local  : {base_dir}\n"
        f"  Remote : {remote_path}\n"
        f"  Prune  : files older than {days} days on remote\n"
        f"  Dry run: {dry_run}",
        expand=False,
    ))

    # ── Discover files ─────────────────────────────────────────────
    _section("Files to sync")
    files = _collect_files(base_dir, patterns)

    if not files:
        console.print(f"  [yellow]No files matching patterns in {base_dir}. Nothing to do.[/]")
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("File", min_width=48)
    table.add_column("Size", justify="right")
    for f in files:
        table.add_row(str(f.relative_to(base_dir)), _human_size(f))
    console.print(table)
    console.print(f"  Total: [bold]{len(files)}[/] file(s)")

    # ── Build include args ─────────────────────────────────────────
    include_args: list[str] = []
    for pat in patterns:
        include_args += ["--include", pat]

    transfer_args = [
        "--create-empty-src-dirs",
        "--transfers", "2",
        "--checkers", "4",
        "--tpslimit", "10",
        "--bwlimit", "2M",
        "--contimeout", "60s",
        "--timeout", "300s",
        "--retries", "3",
        "--low-level-retries", "10",
        *include_args,
    ]

    # ── Upload to Google Drive ─────────────────────────────────────
    _section("Uploading")

    pbar = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TransferSpeedColumn(),
        console=console,
    )

    if dry_run:
        console.print(f"  [dim][DRY RUN] Would copy {base_dir}/ → {remote_path}[/]")
        for f in files:
            console.print(f"  [dim]  {f.relative_to(base_dir)}[/]")
    else:
        # copy() raises on failure — local cleanup below only runs on success
        rclone.copy(
            str(base_dir) + "/",
            remote_path,
            args=transfer_args,
            pbar=pbar,
        )
        console.print("[green]Upload complete.[/]")

    # ── Delete local files (only reached if copy succeeded) ────────
    if delete_local:
        _section("Local cleanup")
        for f in files:
            if dry_run:
                console.print(f"  [dim][DRY RUN] Would remove: {f}[/]")
            else:
                if f.exists():
                    f.unlink()
                    console.print(f"  [dim]Removed: {f}[/]")

    # ── Prune old files on Google Drive ───────────────────────────
    _section(f"Pruning remote files older than {days} days")
    for pat in patterns:
        prune_args = [
            "--drive-use-trash=false",
            "--min-age", f"{days}d",
            "--include", pat,
            "--tpslimit", "10",
            "--transfers", "2",
        ]
        if dry_run:
            prune_args.append("--dry-run")
        console.print(f"  [dim]Pruning pattern: {pat}[/]")
        # delete() is non-fatal per pattern — missing matches are not errors
        try:
            rclone.delete(f"gdrive:{remote}", args=prune_args)
        except Exception as exc:
            console.print(f"  [yellow]Warning: prune failed for '{pat}': {exc}[/]")

    console.print()
    console.print(Panel("[bold green]SYNC COMPLETE[/]", expand=False))
