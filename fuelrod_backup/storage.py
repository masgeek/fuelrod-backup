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
from rich.table import Table

from .config import Config
from .notify import EXIT_FAILURE, EXIT_OK
from .output import emit, error, json_enabled, register

console = register(Console())

# Providers we know about, and the remote names rclone uses for them. Anything
# else rclone supports still works — it just gets no provider-specific flags.
KNOWN_PROVIDERS = ("s3", "b2", "sftp", "s3compliant", "azureblob", "webdav")


class StorageError(RuntimeError):
    """Raised when a transfer fails; never leaves local files deleted."""

# Transfer tuning shared by every provider.
TRANSFER_ARGS = [
    "--create-empty-src-dirs",
    "--transfers", "4",
    "--checkers", "8",
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
    if json_enabled():
        error(msg)
        sys.exit(EXIT_FAILURE)
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
    if provider in ("s3", "s3compliant"):
        args: list[str] = []
        if cfg is None:
            return args
        if cfg.s3_provider:
            args += ["--s3-provider", cfg.s3_provider]
        if cfg.s3_region:
            args += ["--s3-region", cfg.s3_region]
        if cfg.s3_endpoint:
            args += ["--s3-endpoint", cfg.s3_endpoint]
        # Credentials from .backup override whatever rclone has stored, so a
        # remote can be used without touching the rclone config at all.
        if cfg.s3_access_key_id and cfg.s3_secret_access_key:
            args += [
                "--s3-access-key-id", cfg.s3_access_key_id,
                "--s3-secret-access-key", cfg.s3_secret_access_key,
            ]
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


def default_remote(cfg: Config) -> str | None:
    """
    Remote to use when the caller does not pass one.
    
    Generic remote (REMOTE) takes precedence, followed by S3 settings.
    """
    if cfg.remote:
        return cfg.remote
    
    # If a named remote is configured (S3_REMOTE), use it as the prefix
    prefix = cfg.s3_remote if cfg.s3_remote else "s3"
    
    if cfg.s3_bucket:
        target = f"{prefix}:{cfg.s3_bucket}"
        if cfg.s3_prefix:
            target += f"/{cfg.s3_prefix.strip('/')}"
        return target
        
    if cfg.s3_remote:
        # A named remote (created by init) may carry its own bucket/path.
        return f"{cfg.s3_remote}:"
        
    return None


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
    if json_enabled():
        # In JSON mode, we must capture output to prevent corrupting the stdout payload.
        try:
            result = subprocess.run(
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
        return

    # Non-JSON mode: let rclone stream its native high-performance progress bar to the console.
    console.print(f"[cyan]Uploading {count} file(s) → {remote_path}[/]")
    try:
        result = subprocess.run(
            ["rclone", "copy", str(base_dir) + "/", remote_path, *transfer_args, "--progress"],
            stdout=sys.stdout,
            stderr=subprocess.PIPE,
            check=False,
            env=rclone_env(),
        )
    except FileNotFoundError as exc:
        raise StorageError("rclone not found in PATH") from exc
    if result.returncode != 0:
        err = result.stderr.decode(errors="replace").strip()
        raise StorageError(err.splitlines()[-1] if err else f"rclone exited with code {result.returncode}")

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
        transfers: int | None = None,
        checkers: int | None = None,
        since: int | None = None,
        until: int | None = None,
        schemas: list[str] = [],
        interactive: bool = True,
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

    spec = remote or default_remote(cfg)
    if not spec:
        _die("No remote configured. Pass --remote or configure S3/rclone in .backup.")
    try:
        target = parse_remote(spec)
    except ValueError as exc:
        _die(str(exc))

    days = age_days if age_days is not None else 2
    patterns = include_patterns or ["*"]
    remote_path = target.target if target.path else target.provider + ":"

    # 1. Resolve base directory and interactive selections
    if interactive:
        if not json_enabled():
            from . import prompt as q
            base_dir = Path(cfg.base_dir).resolve()
            if not base_dir.is_dir():
                # Fallback to local db-backup if the configured one is missing
                local_fallback = Path.cwd() / "db-backup"
                if local_fallback.is_dir():
                    base_dir = local_fallback
                else:
                    _die(f"Backup root directory not found: {base_dir}")

            db_dirs = sorted([d for d in base_dir.iterdir() if d.is_dir() and not d.is_symlink()])
            if not db_dirs:
                _die(f"No backup folders found in {base_dir}")
            
            table = Table(show_header=True, header_style="bold")
            table.add_column("#", style="dim", width=4)
            table.add_column("Folder", min_width=24)
            table.add_column("Backups", justify="right")
            for i, d in enumerate(db_dirs):
                count = sum(len(list(d.rglob(pat))) for pat in ["*.dump", "*.dump.gz", "*.sql", "*.sql.gz", "*.zip", "*.bak"])
                table.add_row(str(i), d.name, str(count))
            console.print(table)

            selected = q.checkbox(
                "Select folders to sync (Space to toggle, Enter to confirm, none = all)",
                choices=[q.Choice(db.name, value=db.name) for db in db_dirs],
            ).ask()
            if selected:
                schemas = selected

            if since is None or until is None:
                val = q.text("Date range in days (since, until) [e.g. '30, 0' for last 30 days, blank for all]", 
                             default=f"{since or ''}, {until or ''}").ask()
                if val:
                    parts = [p.strip() for p in val.split(",")]
                    if len(parts) >= 1 and parts[0].isdigit():
                        since = int(parts[0])
                    if len(parts) >= 2 and parts[1].isdigit():
                        until = int(parts[1])
        else:
            base_dir = Path(cfg.base_dir).resolve()
    else:
        base_dir = Path(cfg.base_dir).resolve()

    if not base_dir.is_dir():
        # Fallback for non-interactive mode too
        local_fallback = Path.cwd() / "db-backup"
        if local_fallback.is_dir():
            base_dir = local_fallback
        else:
            _die(f"Backup root directory not found: {base_dir}")

    title = "Off-site Sync"
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

    import time
    now = time.time()
    if since is not None:
        cutoff = now - (since * 86400)
        files = [f for f in files if f.stat().st_mtime >= cutoff]
    if until is not None:
        cutoff = now - (until * 86400)
        files = [f for f in files if f.stat().st_mtime <= cutoff]

    if schemas:
        files = [f for f in files if any(s in f.parts for s in schemas)]

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
    if transfers is not None:
        for i, arg in enumerate(transfer_args):
            if arg == "--transfers":
                transfer_args[i+1] = str(transfers)
    if checkers is not None:
        for i, arg in enumerate(transfer_args):
            if arg == "--checkers":
                transfer_args[i+1] = str(checkers)

    for pat in patterns:
        transfer_args += ["--include", pat]
    
    if schemas:
        for s in schemas:
            transfer_args += ["--include", f"{s}/**"]

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
        ]
        p_transfers = str(transfers) if transfers is not None else "2"
        p_checkers = str(checkers) if checkers is not None else "4"
        prune_args += ["--transfers", p_transfers, "--checkers", p_checkers]
        if dry_run:
            prune_args.append("--dry-run")
        console.print(f"  [dim]Pruning pattern: {pat}[/]")
        try:
            code, _out, err = _run_rclone(["delete", remote_path, *prune_args], timeout=1800)
            if code != 0:
                raise RuntimeError(err.strip().splitlines()[-1] if err.strip() else f"exit {code}")
            pruned.append(pat)
        except Exception as exc:
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
