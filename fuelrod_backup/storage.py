"""Off-site sync over rclone remotes (Google Drive, S3, Backblaze B2, SFTP, ...).

The `rclone` binary is invoked directly with list-form argv and an explicit
minimal environment — no shell, and nothing from the caller's environment
(credentials included) reaches the subprocess. The upload mechanics are
identical for every provider; only the remote spec and a couple of
provider-specific flags differ. `Remote` parses ``<provider>:<path>``
and `provider_args()` supplies the per-provider flags (Google Drive needs
trash disabled, S3 needs its provider pinned), so adding a backend is a data
change rather than a code change.

Safety guarantee: local files are deleted only after `rclone.copy()` completes
without raising, so a failed upload can never destroy the local backup.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Column, Table

from .config import Config
from .notify import EXIT_FAILURE, EXIT_OK
from .output import emit, json_enabled, register

console = register(Console())

# Providers we know about, and the remote names rclone uses for them. Anything
# else rclone supports still works — it just gets no provider-specific flags.
KNOWN_PROVIDERS = ("gdrive", "s3", "b2", "sftp", "s3compliant", "azureblob", "webdav")


class StorageError(RuntimeError):
    """Raised when a transfer fails; never leaves local files deleted."""

# Transfer tuning shared by every provider.
TRANSFER_ARGS = [
    "--create-empty-src-dirs",
    "--transfers", "2",
    "--checkers", "4",
    "--tpslimit", "10",
    "--bwlimit", "2M",
    "--contimeout", "60s",
    "--timeout", "300s",
    "--retries", "3",
    "--low-level-retries", "10",
]


@dataclass(frozen=True)
class Remote:
    """A parsed rclone remote: ``provider:path``."""

    spec: str
    provider: str
    path: str

    @property
    def target(self) -> str:
        return f"{self.provider}:{self.path}"


def _section(title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{title}[/]")
    console.print()


def _die(msg: str) -> None:
    console.print(f"[bold red]ERROR:[/] {msg}")
    sys.exit(EXIT_FAILURE)


def _human_size(path: Path) -> str:
    size = path.stat().st_size
    if size >= 1024 * 1024:
        return f"{size / 1024 / 1024:.1f} MB"
    return f"{size / 1024:.1f} KB"


def parse_remote(spec: str) -> Remote:
    """
    Split ``provider:path`` into its parts.

    A bare path (no colon before the first slash) is treated as a local
    destination, which rclone also supports.
    """
    raw = spec.strip()
    if not raw:
        raise ValueError("remote must not be empty")

    head, sep, tail = raw.partition(":")
    if not sep or "/" in head:
        # No provider prefix — a local directory.
        return Remote(spec=raw, provider="local", path=raw)

    provider = head.strip().lower()
    return Remote(spec=raw, provider=provider, path=tail.strip("/"))


def provider_args(provider: str, cfg: Config | None = None) -> list[str]:
    """Provider-specific rclone flags."""
    if provider == "gdrive":
        # Never route pruning through Drive's trash — it keeps a second copy of
        # everything we are trying to reclaim space for.
        return ["--drive-use-trash=false"]
    if provider in ("s3", "s3compliant"):
        args: list[str] = []
        provider_name = getattr(cfg, "s3_provider", "") if cfg else ""
        if provider_name:
            args += ["--s3-provider", provider_name]
        return args
    return []


def rclone_env() -> dict[str, str]:
    """Minimal environment for rclone — the parent env is never inherited."""
    import os

    keys = ("PATH", "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA",
            "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "RCLONE_CONFIG_PASS", "TEMP", "TMP")
    return {key: os.environ[key] for key in keys if key in os.environ}


def rclone_available() -> bool:
    import shutil

    return shutil.which("rclone") is not None


def _run_rclone(args: list[str], *, timeout: int = 3600) -> tuple[int, str, str]:
    """Run rclone with *args*; returns (returncode, stdout, stderr)."""
    import subprocess

    try:
        result = subprocess.run(  # noqa: S603 — list form, fixed binary, no shell
            ["rclone", *args],
            capture_output=True,
            check=False,
            env=rclone_env(),
            timeout=timeout,
        )
    except FileNotFoundError:
        _die("rclone not found in PATH. Install it from https://rclone.org/install/")
    except subprocess.TimeoutExpired:
        _die(f"rclone timed out after {timeout}s: {' '.join(args[:3])}")
    return (
        result.returncode,
        result.stdout.decode(errors="replace"),
        result.stderr.decode(errors="replace"),
    )


def list_remotes() -> list[str]:
    """Remote names rclone has configured."""
    if not rclone_available():
        _die("rclone not found in PATH. Install it from https://rclone.org/install/")
    code, out, err = _run_rclone(["listremotes"], timeout=30)
    if code != 0:
        _die(f"rclone listremotes failed: {err.strip() or out.strip()}")
    return [line.split(":", 1)[0] for line in out.splitlines() if line.strip()]


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


def _upload(base_dir: Path, remote_path: str, transfer_args: list[str], count: int) -> None:
    """
    Upload *base_dir* to *remote_path*.

    Raises StorageError on failure so the caller never deletes local files after
    a failed transfer.
    """
    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}", table_column=Column(min_width=48)),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"[cyan]Uploading {count} file(s) → {remote_path}[/]", total=None)
        try:
            result = subprocess.run(  # noqa: S603 — list form, fixed binary, no shell
                ["rclone", "copy", str(base_dir) + "/", remote_path, *transfer_args],
                capture_output=True,
                check=False,
                env=rclone_env(),
            )
        except FileNotFoundError as exc:
            raise StorageError("rclone not found in PATH") from exc
        if result.returncode != 0:
            err = result.stderr.decode(errors="replace").strip()
            raise StorageError(err.splitlines()[-1] if err else f"rclone exited {result.returncode}")
        progress.update(task, description=f"[green]✓ Uploaded {count} file(s)[/]", total=1, completed=1)

    console.print("[green]Upload complete.[/]")


def run_sync(
        cfg: Config,
        *,
        remote: str | None = None,
        dry_run: bool = False,
        age_days: int | None = None,
        include_patterns: list[str] | None = None,
        delete_local: bool = True,
        show_remotes: bool = False,
) -> int:
    """Sync backups to any rclone remote, then prune old files there."""
    if show_remotes:
        names = list_remotes()
        if json_enabled():
            emit({"command": "sync", "action": "list-remotes", "remotes": names})
        else:
            _section("Configured rclone remotes")
            for name in names or []:
                console.print(f"  {name}:")
            if not names:
                console.print("  [yellow]No remotes configured. Run `rclone config`.[/]")
        return EXIT_OK

    if not rclone_available():
        _die("rclone not found in PATH. Install it from https://rclone.org/install/")

    spec = remote or cfg.gdrive_remote or f"gdrive:{cfg.gdrive_remote}"
    try:
        target = parse_remote(spec)
    except ValueError as exc:
        _die(str(exc))

    days = age_days if age_days is not None else cfg.gdrive_age_days
    patterns = include_patterns or cfg.gdrive_include
    base_dir = Path(cfg.base_dir)
    remote_path = target.target if target.path else target.provider + ":"

    if not base_dir.is_dir():
        _die(f"Backup directory not found: {base_dir}")

    title = "Google Drive Sync" if target.provider == "gdrive" else "Off-site Sync"
    if not json_enabled():
        console.print()
        console.print(Panel(
            f"[bold cyan]{title}[/]\n"
            f"  Local   : {base_dir}\n"
            f"  Remote  : {remote_path}\n"
            f"  Backend : {target.provider}\n"
            f"  Prune   : files older than {days} days on remote\n"
            f"  Dry run : {dry_run}",
            expand=False,
        ))

    _section("Files to sync")
    files = _collect_files(base_dir, patterns)

    if not files:
        message = f"No files matching patterns in {base_dir}. Nothing to do."
        if json_enabled():
            emit({
                "command": "sync", "status": "ok", "remote": remote_path,
                "uploaded": 0, "pruned": False, "detail": message,
            })
        else:
            console.print(f"  [yellow]{message}[/]")
        return EXIT_OK

    if not json_enabled():
        table = Table(show_header=True, header_style="bold")
        table.add_column("File", min_width=48)
        table.add_column("Size", justify="right")
        for f in files:
            table.add_row(str(f.relative_to(base_dir)), _human_size(f))
        console.print(table)
        console.print(f"  Total: [bold]{len(files)}[/] file(s)")

    transfer_args = list(TRANSFER_ARGS) + provider_args(target.provider, cfg)
    for pat in patterns:
        transfer_args += ["--include", pat]

    _section("Uploading")

    removed = 0
    if dry_run:
        console.print(f"  [dim][DRY RUN] Would copy {base_dir}/ → {remote_path}[/]")
        for f in files:
            console.print(f"  [dim]  {f.relative_to(base_dir)}[/]")
    else:
        try:
            _upload(base_dir, remote_path, transfer_args, len(files))
        except StorageError as exc:
            _die(f"Upload failed — no local files were deleted: {exc}")

    if delete_local:
        _section("Local cleanup")
        for f in files:
            if dry_run:
                console.print(f"  [dim][DRY RUN] Would remove: {f}[/]")
            elif f.exists():
                f.unlink()
                removed += 1
                console.print(f"  [dim]Removed: {f}[/]")

    _section(f"Pruning remote files older than {days} days")
    pruned: list[str] = []
    prune_errors: list[str] = []
    for pat in patterns:
        prune_args = [
            *provider_args(target.provider, cfg),
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
            code, _out, err = _run_rclone(["delete", remote_path, *prune_args], timeout=1800)
            if code != 0:
                raise RuntimeError(err.strip().splitlines()[-1] if err.strip() else f"exit {code}")
            pruned.append(pat)
        except Exception as exc:  # noqa: BLE001 — one bad pattern must not abort the rest
            prune_errors.append(f"{pat}: {exc}")
            console.print(f"  [yellow]Warning: prune failed for '{pat}': {exc}[/]")

    status = "warn" if prune_errors else "ok"
    if json_enabled():
        emit({
            "command": "sync",
            "status": status,
            "remote": remote_path,
            "backend": target.provider,
            "files": len(files),
            "uploaded": 0 if dry_run else len(files),
            "removed_locally": removed,
            "pruned_patterns": pruned,
            "prune_errors": prune_errors,
            "dry_run": dry_run,
        })
    else:
        console.print()
        if prune_errors:
            console.print(Panel(
                f"[bold yellow]SYNC COMPLETE — {len(prune_errors)} prune pattern(s) failed[/]\n"
                f"[dim]Upload and local cleanup succeeded.[/]",
                expand=False,
            ))
        else:
            console.print(Panel("[bold green]SYNC COMPLETE[/]", expand=False))

    return EXIT_OK
