"""n8n Docker volume hot-backup logic (ported from database-backup-n8n.sh)."""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from . import prompt as questionary
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .config import Config

console = Console()


# ──────────────────────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────────────────────

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
    if size > 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size} B"


# ──────────────────────────────────────────────────────────────────────────────
#  Container checks and volume introspection
# ──────────────────────────────────────────────────────────────────────────────

def _is_container_running(service: str) -> bool:
    """Return True if a container with the given name is in running state."""
    result = subprocess.run(
        [
            "docker", "ps",
            "--filter", f"name={service}",
            "--filter", "status=running",
            "--format", "{{.Names}}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return any(line.strip() == service for line in result.stdout.splitlines())


def _get_volume_size(volume_name: str) -> str:
    """Return human-readable size of a Docker volume (e.g. '12M')."""
    result = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{volume_name}:/data",
         "alpine", "sh", "-c", "du -sh /data"],
        capture_output=True,
        text=True,
        check=True,
    )
    # Output is "<size>\t/data"
    return result.stdout.split()[0] if result.stdout.strip() else "?"


def _count_db_files(volume_name: str) -> str:
    """Count *.db / *.sqlite files inside a Docker volume."""
    result = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{volume_name}:/data",
         "alpine", "sh", "-c",
         "find /data -name '*.db' -o -name '*.sqlite' 2>/dev/null | wc -l"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _count_workflows(volume_name: str) -> str:
    """Count workflow_*.json files inside a Docker volume."""
    result = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{volume_name}:/data",
         "alpine", "sh", "-c",
         "find /data -name 'workflow_*.json' 2>/dev/null | wc -l"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


# ──────────────────────────────────────────────────────────────────────────────
#  Core backup logic for a single service
# ──────────────────────────────────────────────────────────────────────────────

def _backup_service(service: str, cfg: Config) -> None:
    """Perform a hot backup of one n8n Docker volume."""
    volume_name = f"{service}-data"

    # Use base_dir / "n8n" / service  (not backup_dir which appends db_type)
    backup_dir = Path(cfg.base_dir) / "n8n" / service
    now = datetime.now()
    dated_dir = backup_dir / now.strftime("%Y-%m-%d")
    dated_dir.mkdir(parents=True, exist_ok=True)

    timestamp = now.strftime("%Y%m%d_%H%M%S_%f")[:-3]  # milliseconds
    backup_file = dated_dir / f"{service}_hot_backup_{timestamp}.tar.gz"

    _section(f"Backing up: {service}")

    # 1. Check container is running
    console.print(f"  Checking if [bold]{service}[/] is running...")
    try:
        running = _is_container_running(service)
    except subprocess.CalledProcessError as exc:
        console.print(f"  [yellow]WARN:[/] Could not query Docker: {exc}. Skipping.")
        return

    if not running:
        console.print(f"  [yellow]WARN:[/] Container '[bold]{service}[/]' is not running. Skipping.")
        return

    # 2. Gather volume info
    console.print(f"  Gathering volume info for [bold]{volume_name}[/]...")
    try:
        vol_size = _get_volume_size(volume_name)
        db_files = _count_db_files(volume_name)
        workflow_count = _count_workflows(volume_name)
    except subprocess.CalledProcessError as exc:
        _die(f"Failed to query volume '{volume_name}': {exc}")

    console.print(f"  Volume size     : [cyan]{vol_size}[/]")
    console.print(f"  Database files  : [cyan]{db_files}[/]")
    console.print(f"  Workflow count  : [cyan]{workflow_count}[/]")

    # 3. Create tar backup (hot — no container stop)
    console.print(f"  Creating snapshot for [bold]{volume_name}[/]...")
    try:
        subprocess.run(
            [
                "docker", "run", "--rm",
                "-v", f"{volume_name}:/source_data",
                "-v", f"{str(dated_dir)}:/backup",
                "alpine", "sh", "-c",
                (
                    f"mkdir -p /backup/temp_snapshot && "
                    f"cp -a /source_data/. /backup/temp_snapshot/ && "
                    f"tar -czf /backup/{backup_file.name} -C /backup temp_snapshot && "
                    f"rm -rf /backup/temp_snapshot"
                ),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        _die(
            f"Backup failed for '{service}' (exit {exc.returncode}).\n"
            f"  stderr: {exc.stderr.strip()}"
        )

    # 4. Write summary file
    backup_size = _human_size(backup_file)
    summary_file = dated_dir / f"backup_summary_{timestamp}.txt"
    summary_file.write_text(
        "\n".join([
            f"Backup Date: {now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}",
            f"Backup Type: Hot Backup (No Downtime)",
            f"Service: {service}",
            f"Source Volume: {volume_name}",
            f"Volume Size: {vol_size}",
            f"Database Files: {db_files}",
            f"Workflow Count: {workflow_count}",
            f"Backup File: {backup_file}",
            f"Backup Size: {backup_size}",
            "NOTE: This is a hot backup. Small risk of inconsistency if data was being written.",
        ]),
        encoding="utf-8",
    )

    console.print(f"  [green]✓[/] Backup complete: [bold]{backup_file.name}[/]  ([dim]{backup_size}[/])")
    console.print(f"  Summary: [dim]{summary_file}[/]")

    # 5. Cleanup old dated subdirs
    _cleanup_old(backup_dir, cfg.days_to_keep)


def _cleanup_old(backup_dir: Path, days_to_keep: int) -> None:
    """Remove dated subdirectories older than days_to_keep days (0 = skip)."""
    if days_to_keep <= 0:
        return

    cutoff = datetime.now() - timedelta(days=days_to_keep)
    removed = 0
    for candidate in sorted(backup_dir.iterdir()):
        if not candidate.is_dir():
            continue
        # Match YYYY-MM-DD pattern
        try:
            folder_date = datetime.strptime(candidate.name, "%Y-%m-%d")
        except ValueError:
            continue
        if folder_date < cutoff:
            import shutil
            shutil.rmtree(candidate)
            console.print(f"  [dim]Removed old backup dir: {candidate.name}[/]")
            removed += 1

    if removed == 0:
        console.print(f"  [dim]No old backups to clean (retention: {days_to_keep} days)[/]")


# ──────────────────────────────────────────────────────────────────────────────
#  Public entry point
# ──────────────────────────────────────────────────────────────────────────────

def run_n8n_backup(
    cfg: Config,
    *,
    interactive: bool = True,
    services: list[str] | None = None,
) -> None:
    """Back up n8n Docker volumes.

    Args:
        cfg:         Resolved configuration.
        interactive: When True, show a service table and ask for confirmation.
        services:    Explicit list of services to back up. None = use cfg.n8n_services.
    """
    # Resolve service list: CLI override → config → default
    candidates: list[str] = services if services else cfg.n8n_services

    # Apply skip list
    active_services = [s for s in candidates if s not in cfg.skip_services]

    if not active_services:
        _die("No services to back up (all are skipped or the list is empty).")

    console.print(Panel("[bold cyan]n8n Volume Backup[/]", expand=False))

    if interactive:
        # Show services table
        _section("Services to Back Up")
        table = Table(show_header=True, header_style="bold")
        table.add_column("#", style="dim", width=4)
        table.add_column("Service", min_width=20)
        table.add_column("Volume", min_width=24)
        table.add_column("Skipped?", justify="center")
        for i, svc in enumerate(candidates):
            skipped = svc in cfg.skip_services
            skip_marker = "[yellow]yes[/]" if skipped else "[green]no[/]"
            table.add_row(str(i + 1), svc, f"{svc}-data", skip_marker)
        console.print(table)

        if not questionary.confirm("Proceed with backup?", default=True).ask():
            console.print("[yellow]Aborted by user.[/]")
            sys.exit(0)

    for service in active_services:
        _backup_service(service, cfg)

    console.print()
    console.print(Panel("[bold green]n8n BACKUP COMPLETE[/]", expand=False))
