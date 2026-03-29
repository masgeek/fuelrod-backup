"""Google Drive sync via rclone (replaces gbk.sh)."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
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


def _rclone(*args: str, dry_run: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    cmd = ["rclone"] + list(args)
    if dry_run:
        cmd.append("--dry-run")
    return subprocess.run(cmd, check=check)


def _collect_files(base_dir: Path, patterns: list[str]) -> list[Path]:
    """Return all files under base_dir matching any of the glob patterns."""
    found: list[Path] = []
    for pattern in patterns:
        found.extend(base_dir.rglob(pattern))
    # Deduplicate while preserving order
    seen: set[Path] = set()
    result: list[Path] = []
    for f in found:
        if f not in seen and f.is_file():
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
    Sync local backups to Google Drive via rclone, then prune old remote files.

    Safety guarantee (fixes gbk.sh risk): local files are only deleted AFTER
    rclone copy exits with code 0. If the copy fails, the exception propagates
    and no local files are removed.
    """
    if not shutil.which("rclone"):
        _die("rclone not found in PATH. Install it from https://rclone.org/install/")

    remote = gdrive_remote or cfg.gdrive_remote
    days = age_days if age_days is not None else cfg.gdrive_age_days
    patterns = include_patterns or cfg.gdrive_include
    base_dir = Path(cfg.base_dir)

    if not base_dir.is_dir():
        _die(f"Backup directory not found: {base_dir}")

    remote_path = f"gdrive:{remote}/"

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

    # ── Ensure remote folder exists ────────────────────────────────
    _section("Prepare remote")
    if dry_run:
        console.print(f"  [dim][DRY RUN] Would create remote folder: {remote_path}[/]")
    else:
        console.print(f"  Creating remote folder if needed: {remote_path}")
        subprocess.run(["rclone", "mkdir", remote_path], check=True)

    # ── Copy to Google Drive ───────────────────────────────────────
    _section("Uploading")
    include_args: list[str] = []
    for pat in patterns:
        include_args += ["--include", pat]

    copy_cmd = [
        "rclone", "copy", str(base_dir) + "/", remote_path,
        *include_args,
        "--verbose", "--progress", "--create-empty-src-dirs",
        "--transfers", "2", "--checkers", "4",
        "--tpslimit", "10", "--bwlimit", "2M",
        "--contimeout", "60s", "--timeout", "300s",
        "--retries", "3", "--low-level-retries", "10",
    ]
    if dry_run:
        copy_cmd.append("--dry-run")

    console.print(f"  [dim]{' '.join(copy_cmd[:6])} ...[/]")
    # check=True ensures we raise on failure — local cleanup below only runs on success
    subprocess.run(copy_cmd, check=True)
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
        prune_cmd = [
            "rclone", "--drive-use-trash=false", "--verbose",
            "--min-age", f"{days}d", "--include", pat,
            "--tpslimit", "10", "--transfers", "2",
            "delete", f"gdrive:{remote}",
        ]
        if dry_run:
            prune_cmd.append("--dry-run")
        console.print(f"  [dim]Pruning pattern: {pat}[/]")
        subprocess.run(prune_cmd, check=False)  # non-fatal if pattern matches nothing

    console.print()
    console.print(Panel("[bold green]SYNC COMPLETE[/]", expand=False))
