"""n8n Docker volume restore logic (ported from database-restore-n8n.sh)."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tarfile
import tempfile
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


def _read_summary_field(summary_file: Path, field: str) -> str | None:
    """Read a single field value from a backup_summary_*.txt file."""
    try:
        for line in summary_file.read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{field}:"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return None


# ──────────────────────────────────────────────────────────────────────────────
#  Step 1 — Select service
# ──────────────────────────────────────────────────────────────────────────────

def _select_service(cfg: Config) -> tuple[str, str, Path]:
    """Interactively select an n8n service. Returns (service, volume_name, service_backup_dir)."""
    _section("Step 1 — Select Service")

    services = cfg.n8n_services
    if not services:
        _die("No n8n services configured (N8N_SERVICES is empty).")

    table = Table(show_header=True, header_style="bold")
    table.add_column("#", style="dim", width=4)
    table.add_column("Service", min_width=20)
    table.add_column("Volume", min_width=24)
    for i, svc in enumerate(services):
        table.add_row(str(i + 1), svc, f"{svc}-data")
    console.print(table)

    choices = [questionary.Choice(title=svc, value=svc) for svc in services]
    selected_service: str = questionary.select(
        "Select service to restore", choices=choices
    ).ask()

    volume_name = f"{selected_service}-data"
    service_backup_dir = Path(cfg.base_dir) / "n8n" / selected_service

    console.print(f"  Service : [bold]{selected_service}[/]")
    console.print(f"  Volume  : [bold]{volume_name}[/]")
    console.print(f"  Backups : [dim]{service_backup_dir}[/]")

    return selected_service, volume_name, service_backup_dir


# ──────────────────────────────────────────────────────────────────────────────
#  Step 2 — Select backup file
# ──────────────────────────────────────────────────────────────────────────────

def _select_backup_file(service_backup_dir: Path, service: str) -> Path:
    """Interactively select a backup .tar.gz file. Returns its Path."""
    _section("Step 2 — Select Backup File")

    if not service_backup_dir.is_dir():
        _die(f"Backup directory not found: {service_backup_dir}")

    # List date subdirs matching YYYY-MM-DD, sorted newest-first
    date_dirs = sorted(
        [d for d in service_backup_dir.iterdir() if d.is_dir() and _is_date_dir(d.name)],
        reverse=True,
    )
    if not date_dirs:
        _die(f"No backup date folders found in {service_backup_dir}")

    # Build choices with backup count hint
    date_choices = [
        questionary.Choice(
            title=f"{d.name}  ({len(list(d.glob('*.tar.gz')))} backup(s))",
            value=d,
        )
        for d in date_dirs
    ]
    selected_date_dir: Path = questionary.select(
        "Select backup date", choices=date_choices
    ).ask()

    # List .tar.gz files in selected date folder, newest-first
    backups = sorted(selected_date_dir.glob("*.tar.gz"), reverse=True)
    if not backups:
        _die(f"No .tar.gz backups found in {selected_date_dir}")

    # Build choices enriched with summary metadata
    backup_choices: list[questionary.Choice] = []
    for bf in backups:
        label = _build_backup_label(bf)
        backup_choices.append(questionary.Choice(title=label, value=bf))

    selected_backup: Path = questionary.select(
        "Select backup file (newest first)",
        choices=backup_choices,
        default=backup_choices[0],
    ).ask()

    console.print(f"  Selected: [bold]{selected_backup.name}[/]  ([dim]{_human_size(selected_backup)}[/])")
    return selected_backup


def _is_date_dir(name: str) -> bool:
    """Return True if name matches YYYY-MM-DD."""
    import re
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", name))


def _build_backup_label(backup_file: Path) -> str:
    """Build a display label for a .tar.gz file, reading summary if present."""
    size = _human_size(backup_file)
    # Find matching summary: same stem prefix before .tar.gz, ends with _summary_<timestamp>.txt
    # Convention: service_hot_backup_TIMESTAMP.tar.gz → backup_summary_TIMESTAMP.txt
    timestamp = _extract_timestamp_from_backup(backup_file)
    summary_label = ""
    if timestamp:
        summary_file = backup_file.parent / f"backup_summary_{timestamp}.txt"
        if summary_file.exists():
            workflows = _read_summary_field(summary_file, "Workflow Count")
            db_files = _read_summary_field(summary_file, "Database Files")
            parts = []
            if workflows is not None:
                parts.append(f"workflows={workflows}")
            if db_files is not None:
                parts.append(f"dbs={db_files}")
            if parts:
                summary_label = f"  [{', '.join(parts)}]"
    return f"{backup_file.name} — {size}{summary_label}"


def _extract_timestamp_from_backup(backup_file: Path) -> str | None:
    """
    Extract the timestamp suffix from a filename like:
      n8n_hot_backup_20240101_120000_123.tar.gz → 20240101_120000_123
    """
    stem = backup_file.name
    # Strip .tar.gz
    if stem.endswith(".tar.gz"):
        stem = stem[:-7]
    # The timestamp is everything after the last occurrence of "_hot_backup_"
    marker = "_hot_backup_"
    idx = stem.find(marker)
    if idx != -1:
        return stem[idx + len(marker):]
    return None


# ──────────────────────────────────────────────────────────────────────────────
#  Step 3 — Confirm
# ──────────────────────────────────────────────────────────────────────────────

def _confirm_restore(
    service: str,
    volume_name: str,
    backup_file: Path,
    dry_run: bool,
) -> None:
    """Print summary panel and ask for explicit confirmation."""
    _section("Step 3 — Confirm Restore")

    console.print(Panel(
        "\n".join([
            f"  Service     : [bold]{service}[/]",
            f"  Container   : [bold]{service}[/]",
            f"  Volume      : [bold]{volume_name}[/]",
            f"  Backup file : [bold]{backup_file.name}[/]",
            f"  Size        : {_human_size(backup_file)}",
            f"  Dry run     : {'[yellow]yes[/]' if dry_run else '[green]no[/]'}",
        ]),
        title="[bold]Restore Plan[/]",
        expand=False,
    ))

    console.print(
        f"\n  [bold red]WARNING:[/] This will REPLACE ALL CURRENT DATA "
        f"in the [bold]{volume_name}[/] volume!\n"
    )

    if not questionary.confirm("Proceed with restore?", default=False).ask():
        console.print("[yellow]Aborted by user.[/]")
        sys.exit(0)


# ──────────────────────────────────────────────────────────────────────────────
#  Step 4 — Execute restore
# ──────────────────────────────────────────────────────────────────────────────

def _execute_restore(
    service: str,
    volume_name: str,
    backup_file: Path,
    service_backup_dir: Path,
    *,
    verbose: bool,
) -> None:
    """Perform the actual volume restore. All steps use check=True."""
    from datetime import datetime

    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pre_restore_dir = service_backup_dir / f"pre_restore_{current_timestamp}"
    pre_restore_dir.mkdir(parents=True, exist_ok=True)

    temp_dir = Path(tempfile.mkdtemp(prefix=f"{service}_restore_"))
    try:
        # Step 1 — Stop container
        console.print(f"  Stopping container [bold]{service}[/]...")
        try:
            subprocess.run(["docker", "stop", service], capture_output=True, text=True, check=True)
            console.print(f"  Container stopped.")
        except subprocess.CalledProcessError as exc:
            console.print(
                f"  [yellow]WARN:[/] Failed to stop container '{service}': "
                f"{exc.stderr.strip()}. Continuing..."
            )

        # Step 2 — Pre-restore backup
        console.print(f"  Creating pre-restore backup → [dim]{pre_restore_dir}[/]...")
        subprocess.run(
            [
                "docker", "run", "--rm",
                "-v", f"{volume_name}:/data",
                "-v", f"{str(pre_restore_dir)}:/backup",
                "alpine",
                "tar", "-czf", "/backup/pre_restore_backup.tar.gz", "/data",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        console.print(f"  Pre-restore backup stored at: [dim]{pre_restore_dir / 'pre_restore_backup.tar.gz'}[/]")

        # Step 3 — Clear volume
        console.print(f"  Clearing volume [bold]{volume_name}[/]...")
        subprocess.run(
            [
                "docker", "run", "--rm",
                "-v", f"{volume_name}:/data",
                "alpine", "sh", "-c", "rm -rf /data/*",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        # Step 4 — Extract backup to temp dir (host-side)
        console.print(f"  Extracting [bold]{backup_file.name}[/] → [dim]{temp_dir}[/]...")
        with tarfile.open(backup_file) as tf:
            tf.extractall(temp_dir)

        # Step 5 — Determine source dir
        temp_snapshot = temp_dir / "temp_snapshot"
        temp_data = temp_dir / "data"
        if temp_snapshot.is_dir():
            src_dir = temp_snapshot
        elif temp_data.is_dir():
            src_dir = temp_data
        else:
            src_dir = temp_dir

        if verbose:
            console.print(f"  [dim]Source dir resolved: {src_dir}[/]")

        # Step 6 — Copy files to volume
        if verbose:
            console.print(f"  [dim]Copying {src_dir} → volume {volume_name}[/]")
        console.print(f"  Copying data to volume [bold]{volume_name}[/]...")
        subprocess.run(
            [
                "docker", "run", "--rm",
                "-v", f"{volume_name}:/data",
                "-v", f"{str(src_dir)}:/restore",
                "alpine", "sh", "-c", "cp -a /restore/. /data/",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        # Step 7 — Set permissions
        console.print(f"  Setting permissions (1000:1000) on volume [bold]{volume_name}[/]...")
        subprocess.run(
            [
                "docker", "run", "--rm",
                "-v", f"{volume_name}:/data",
                "alpine", "sh", "-c", "chown -R 1000:1000 /data",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        # Step 8 — Start container
        console.print(f"  Starting container [bold]{service}[/]...")
        subprocess.run(
            ["docker", "start", service],
            capture_output=True,
            text=True,
            check=True,
        )
        console.print(f"  Container started.")

    finally:
        # Step 9 — Clean temp dir
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            if verbose:
                console.print(f"  [dim]Temporary directory cleaned: {temp_dir}[/]")


def _dry_run_restore(
    service: str,
    volume_name: str,
    backup_file: Path,
    service_backup_dir: Path,
    *,
    verbose: bool,
) -> None:
    """Print what would happen for each restore step without making any changes."""
    _section("Dry Run — Restore Plan")

    from datetime import datetime
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pre_restore_dir = service_backup_dir / f"pre_restore_{current_timestamp}"

    steps = [
        f"Stop container: docker stop {service}",
        f"Create pre-restore backup → {pre_restore_dir}/pre_restore_backup.tar.gz",
        f"Clear volume: docker run --rm -v {volume_name}:/data alpine sh -c 'rm -rf /data/*'",
        f"Extract backup (host-side): tarfile.open({backup_file}).extractall(<temp_dir>)",
        "Determine source dir: check temp_snapshot → data → temp_dir root",
        f"Copy files: docker run --rm -v {volume_name}:/data -v <src_dir>:/restore alpine sh -c 'cp -a /restore/. /data/'",
        f"Set permissions: docker run --rm -v {volume_name}:/data alpine sh -c 'chown -R 1000:1000 /data'",
        f"Start container: docker start {service}",
        "Clean temporary extraction directory",
    ]

    for i, step in enumerate(steps, 1):
        console.print(f"  [dim][DRY-RUN][/] Step {i}: {step}")

    console.print()
    console.print("[yellow]Dry run complete. No changes were made.[/]")


# ──────────────────────────────────────────────────────────────────────────────
#  Public entry point
# ──────────────────────────────────────────────────────────────────────────────

def run_n8n_restore(
    cfg: Config,
    *,
    service: str | None = None,
    backup_file: Path | None = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """Restore an n8n Docker volume from a backup.

    Args:
        cfg:         Resolved configuration.
        service:     Service name to restore. If None, prompt interactively.
        backup_file: Path to .tar.gz to restore directly. If None, prompt interactively.
        dry_run:     Show plan only — no changes made.
        verbose:     Print detailed step logs.
    """
    console.print(Panel("[bold cyan]n8n Volume Restore[/]", expand=False))

    # Step 1 — Select service
    if service is not None:
        volume_name = f"{service}-data"
        service_backup_dir = Path(cfg.base_dir) / "n8n" / service
        console.print(f"  Service : [bold]{service}[/]")
        console.print(f"  Volume  : [bold]{volume_name}[/]")
    else:
        service, volume_name, service_backup_dir = _select_service(cfg)

    # Step 2 — Select backup file
    if backup_file is not None:
        if not backup_file.exists():
            _die(f"Backup file not found: {backup_file}")
        console.print(f"  Backup file: [bold]{backup_file.name}[/]  ([dim]{_human_size(backup_file)}[/])")
    else:
        backup_file = _select_backup_file(service_backup_dir, service)

    # Step 3 — Confirm
    _confirm_restore(service, volume_name, backup_file, dry_run)

    # Step 4 — Execute (or dry run)
    _section("Step 4 — Execute Restore")

    if dry_run:
        _dry_run_restore(service, volume_name, backup_file, service_backup_dir, verbose=verbose)
        return

    try:
        _execute_restore(
            service,
            volume_name,
            backup_file,
            service_backup_dir,
            verbose=verbose,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else ""
        _die(
            f"Restore failed (exit {exc.returncode})."
            + (f"\n  stderr: {stderr}" if stderr else "")
        )

    console.print()
    console.print(Panel("[bold green]n8n RESTORE COMPLETE[/]", expand=False))
    console.print(f"  Backup used : [bold]{backup_file}[/]")
