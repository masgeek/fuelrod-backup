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
from .fileutil import human_size
from .notify import EXIT_FAILURE, EXIT_OK
from .output import emit, error, json_enabled, register, warn

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

    @property
    def location(self) -> str:
        """What to hand to rclone: a plain folder for local destinations, ``provider:path`` otherwise."""
        if self.provider == "local":
            return str(Path(self.path).expanduser())
        return self.target if self.path else self.provider + ":"


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
    return human_size(path.stat().st_size)


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
    drive_letter = len(head) == 1 and head.isalpha() and tail[:1] in ("/", chr(92))
    if not sep or "/" in head or drive_letter:
        # No provider prefix (or a Windows drive such as D:/OneDrive) — a local directory.
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

    # SystemRoot/windir: without them Go (and so rclone) cannot resolve DNS on Windows
    # ("getaddrinfow: A non-recoverable error occurred during a database lookup").
    keys = ("PATH", "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA",
            "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "RCLONE_CONFIG_PASS", "TEMP", "TMP",
            "SystemRoot", "windir", "ProgramData")
    return {key: os.environ[key] for key in keys if key in os.environ}


_RCLONE = "rclone"


def use_rclone(command: str | None) -> None:
    """Choose the rclone executable (name on PATH or full path, e.g. D:/tools/rclone.exe)."""
    global _RCLONE
    _RCLONE = (command or "").strip().strip('"') or "rclone"


def rclone_bin() -> str:
    """The rclone executable to run (RCLONE_CMD / --rclone, default ``rclone``)."""
    return _RCLONE


def rclone_available() -> bool:
    import shutil

    return shutil.which(_RCLONE) is not None


_VERBOSE = 0
_SECRET_FLAGS = ("--s3-secret-access-key", "--s3-access-key-id", "--sftp-pass", "--pass", "--password")
_TRANSFER_COMMANDS = ("copy", "sync", "move", "check", "delete", "deletefile")


def set_verbose(level: int) -> None:
    """0 = quiet, 1 = log every rclone command and file (-v), 2 = rclone debug output (-vv)."""
    global _VERBOSE
    _VERBOSE = max(0, int(level))


def verbose_level() -> int:
    return _VERBOSE


def mask_args(args: list[str]) -> list[str]:
    """Copy of *args* with credential values hidden, safe to print."""
    masked: list[str] = []
    hide = False
    for arg in args:
        if hide:
            masked.append("***")
            hide = False
        elif arg in _SECRET_FLAGS:
            masked.append(arg)
            hide = True
        elif arg.split("=", 1)[0] in _SECRET_FLAGS and "=" in arg:
            masked.append(arg.split("=", 1)[0] + "=***")
        else:
            masked.append(arg)
    return masked


def log_line(text: str) -> None:
    """Verbose output: on stderr in JSON mode (stdout stays one JSON document), else on the console."""
    if json_enabled():
        print(text, file=sys.stderr, flush=True)
    else:
        console.print(text, markup=False, highlight=False, soft_wrap=True)


def _verbose_flags(command: str) -> list[str]:
    if not _VERBOSE or command not in _TRANSFER_COMMANDS:
        return []
    return ["-vv" if _VERBOSE >= 2 else "-v", "--stats", "5s"]


def run_rclone_process(args: list[str], timeout: float | None, label: str = "") -> subprocess.CompletedProcess:
    """
    Run rclone and return the finished process (stdout/stderr captured).

    With verbose on, the command is echoed first (credentials masked) and rclone's log — every file
    copied, skipped or deleted, plus periodic transfer stats — is streamed live instead of being
    held back until the process exits. *label* prefixes the lines so parallel jobs stay readable.
    """
    import threading

    full = [rclone_bin(), *args, *_verbose_flags(args[0])]
    if not _VERBOSE:
        return subprocess.run(  # noqa: S603 — list form, fixed binary, no shell
            full, capture_output=True, check=False, env=rclone_env(), timeout=timeout, stdin=subprocess.DEVNULL,
        )

    prefix = f"[{label}] " if label else ""
    secrets = [args[i + 1] for i, a in enumerate(args[:-1]) if a in _SECRET_FLAGS and args[i + 1]]
    shown = [rclone_bin(), *mask_args(args), *_verbose_flags(args[0])]
    log_line(prefix + "$ " + " ".join(f'"{a}"' if " " in a else a for a in shown))

    proc = subprocess.Popen(  # noqa: S603 — list form, fixed binary, no shell
        full, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL, env=rclone_env(),
    )
    out_chunks: list[bytes] = []
    err_chunks: list[bytes] = []

    def pump_err() -> None:
        for raw in iter(proc.stderr.readline, b""):
            err_chunks.append(raw)
            line = raw.decode(errors="replace").rstrip()
            for secret in secrets:
                line = line.replace(secret, "***")
            if line:
                log_line(prefix + line)

    def pump_out() -> None:
        out_chunks.append(proc.stdout.read())

    threads = [threading.Thread(target=pump_err, daemon=True), threading.Thread(target=pump_out, daemon=True)]
    for t in threads:
        t.start()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise
    finally:
        for t in threads:
            t.join(timeout=5)
    return subprocess.CompletedProcess(full, proc.returncode, b"".join(out_chunks), b"".join(err_chunks))


def _run_rclone(args: list[str], *, timeout: int = 3600) -> tuple[int, str, str]:
    """Run rclone with *args*; returns (returncode, stdout, stderr)."""
    try:
        result = run_rclone_process(args, timeout)
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
            if f.name == "manifest.json" and f.parent == base_dir:
                continue  # the index is regenerated at the destination, not copied as a backup
            if f.is_file() and f not in seen:
                seen.add(f)
                result.append(f)
    return sorted(result)


_ARTIFACT_SUFFIXES = (".dump", ".dump.gz", ".sql", ".sql.gz", ".zip", ".bak", ".age", ".gpg")


def _is_artifact(path: Path) -> bool:
    return path.name.endswith(_ARTIFACT_SUFFIXES) and not path.name.startswith("manifest_")


def _mtime(f) -> float:
    return f.mtime if hasattr(f, "mtime") else f.stat().st_mtime


def select_latest(files: list, latest: int) -> list:
    """
    Keep only the newest *latest* backups of each database folder.

    Each backup's manifest travels with it, and when an incremental dump is selected its full
    base is kept too (an increment is useless without it). Other files (state sidecars, notes)
    are not kept. Works on local ``Path`` objects and on remote listings (``.mtime`` attribute).
    """
    if latest <= 0:
        return list(files)

    by_dir: dict = {}
    for f in files:
        by_dir.setdefault(f.parent, []).append(f)

    def order(f):
        return (_mtime(f), f.name)

    kept: list = []
    for group in by_dir.values():
        artifacts = sorted((f for f in group if _is_artifact(f)), key=order)
        chosen = artifacts[-latest:]
        increments = [f for f in chosen if "_inc." in f.name]
        if increments:
            oldest = min(_mtime(p) for p in increments)
            fulls = [f for f in artifacts if "_inc." not in f.name and _mtime(f) <= oldest]
            base = (fulls or [f for f in artifacts if "_inc." not in f.name])[-1:]
            chosen = sorted({*chosen, *base}, key=order)
        kept.extend(chosen)
        for manifest in (f for f in group if f.name.startswith("manifest_") and f.name.endswith(".txt")):
            stamp = manifest.name[len("manifest_"):-len(".txt")]
            if any(stamp in c.name for c in chosen):
                kept.append(manifest)
    return sorted(kept, key=lambda f: (str(f.parent), f.name))


def _remove_empty_local_dirs(base_dir: Path) -> None:
    """Pushing with local deletion leaves empty engine/database folders behind; tidy them up."""
    from .fileutil import remove_empty_dirs

    gone = remove_empty_dirs(base_dir)
    if gone and not json_enabled():
        console.print(f"  [dim]Removed {len(gone)} empty folder(s).[/]")


def _refresh_local_manifest(base_dir: Path) -> None:
    from .manifest import write_local

    try:
        write_local(base_dir)
    except OSError as exc:
        warn(f"could not update manifest.json: {exc}")


def _refresh_destination_manifest(remote_path: str, extra: list[str]) -> None:
    """List the destination once and publish manifest.json there so `pull` never has to traverse it."""
    from .manifest import refresh_remote

    try:
        refresh_remote(remote_path, extra)
    except StorageError as exc:
        warn(f"could not update the manifest on {remote_path}: {exc}")


def _upload(base_dir: Path, remote_path: str, transfer_args: list[str], count: int) -> None:
    """
    Upload *base_dir* to *remote_path*.

    Raises StorageError on failure so the caller never deletes local files after
    a failed transfer.
    """
    if _VERBOSE:
        console.print(f"[cyan]Uploading {count} file(s) → {remote_path}[/]") if not json_enabled() else None
        try:
            result = run_rclone_process(["copy", str(base_dir) + "/", remote_path, *transfer_args], None)
        except FileNotFoundError as exc:
            raise StorageError("rclone not found in PATH") from exc
        if result.returncode != 0:
            err = result.stderr.decode(errors="replace").strip()
            raise StorageError(err.splitlines()[-1] if err else f"rclone exited {result.returncode}")
        return

    if json_enabled():
        # In JSON mode, we must capture output to prevent corrupting the stdout payload.
        try:
            result = subprocess.run(
                [rclone_bin(), "copy", str(base_dir) + "/", remote_path, *transfer_args],
                capture_output=True,
                check=False,
                env=rclone_env(),
                stdin=subprocess.DEVNULL,
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
            [rclone_bin(), "copy", str(base_dir) + "/", remote_path, *transfer_args, "--progress"],
            stdout=sys.stdout,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            check=False,
            env=rclone_env(),
        )
    except FileNotFoundError as exc:
        raise StorageError("rclone not found in PATH") from exc
    if result.returncode != 0:
        err = result.stderr.decode(errors="replace").strip()
        raise StorageError(err.splitlines()[-1] if err else f"rclone exited with code {result.returncode}")

    console.print("[green]Upload complete.[/]")


def _sync_one(
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
        latest: int | None = None,
        quota: str | None = None,
        collect: list | None = None,
        out: dict | None = None,
) -> int:
    """Sync backups to one destination (rclone remote or local folder), then prune old files there."""

    def _out(payload: dict) -> None:
        if collect is not None:
            collect.append(payload)
        else:
            emit(payload)

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

    if not remote and cfg.push_remote:
        remote = cfg.push_remote  # PUSH_REMOTE: a configured rclone remote
    spec = remote or default_remote(cfg)
    if not spec:
        _die("No remote configured. Pass --remote, or set PUSH_REMOTE / REMOTE / S3_* (see `push-init`).")
    try:
        target = parse_remote(spec)
    except ValueError as exc:
        _die(str(exc))

    quota_text = (quota if quota is not None else cfg.push_quota).strip()
    quota_bytes = 0
    if quota_text and quota_text != "0":
        from .quota import parse_size

        try:
            quota_bytes = parse_size(quota_text)
        except ValueError as exc:
            _die(str(exc))
    # Age pruning only runs when --days is given. It used to default to 2 days, which deleted freshly
    # uploaded backups that were older than that straight away; use --days or a quota to limit the remote.
    days = age_days if age_days is not None else 0
    prune_by_age = age_days is not None
    prune_text = f"files older than {days} days on remote" if prune_by_age else "off (set --days N or a quota)"
    patterns = include_patterns or ["*"]
    remote_path = target.location
    keep_latest = cfg.push_latest if latest is None else max(0, latest)

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
                patterns = ["*.dump", "*.dump.gz", "*.sql", "*.sql.gz", "*.zip", "*.bak"]
                count = sum(len(list(d.rglob(pat))) for pat in patterns)
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

    if target.provider == "local":
        dest_dir = Path(remote_path).resolve()
        if dest_dir == base_dir or base_dir in dest_dir.parents:
            _die(f"Destination {dest_dir} is inside the backup folder {base_dir}; choose a folder outside it.")

    title = "Off-site Sync"
    if not json_enabled():
        console.print()
        console.print(Panel(
            f"[bold cyan]{title}[/]\n"
            f"  Local   : {base_dir}\n"
            f"  Remote  : {remote_path}\n"
            f"  Backend : {target.provider}\n"
            f"  Latest  : {keep_latest or 'all'} backup(s) per database\n"
            f"  Prune   : {prune_text}\n"
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

    if keep_latest:
        files = select_latest(files, keep_latest)
    if out is not None:
        out["files"], out["base_dir"] = files, base_dir

    if not files:
        message = f"No files matching patterns in {base_dir}. Nothing to do."
        if json_enabled():
            _out({
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
    if target.provider == "local":  # the bandwidth/rate limits are for network uploads
        for flag in ("--bwlimit", "--tpslimit"):
            if flag in transfer_args:
                i = transfer_args.index(flag)
                del transfer_args[i:i + 2]
    if transfers is not None:
        for i, arg in enumerate(transfer_args):
            if arg == "--transfers":
                transfer_args[i+1] = str(transfers)
    if checkers is not None:
        for i, arg in enumerate(transfer_args):
            if arg == "--checkers":
                transfer_args[i+1] = str(checkers)

    if not keep_latest:  # with --latest the exact file list is passed via --files-from instead
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
        files_from: Path | None = None
        if keep_latest:
            import tempfile

            with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as fh:
                fh.write("\n".join(f.relative_to(base_dir).as_posix() for f in files) + "\n")
                files_from = Path(fh.name)
            transfer_args += ["--files-from", str(files_from)]
        try:
            _upload(base_dir, remote_path, transfer_args, len(files))
        except StorageError as exc:
            _die(f"Upload failed — no local files were deleted: {exc}")
        finally:
            if files_from is not None:
                files_from.unlink(missing_ok=True)

    if delete_local:
        _section("Local cleanup")
        for f in files:
            if dry_run:
                console.print(f"  [dim][DRY RUN] Would remove: {f}[/]")
            elif f.exists():
                f.unlink()
                removed += 1
                console.print(f"  [dim]Removed: {f}[/]")
        if removed:
            _remove_empty_local_dirs(base_dir)
        if removed and cfg.manifest:
            _refresh_local_manifest(base_dir)

    pruned: list[str] = []
    prune_errors: list[str] = []
    if prune_by_age:
        _section(f"Pruning remote files older than {days} days")
    for pat in patterns if prune_by_age else []:
        prune_args = [
            *provider_args(target.provider, cfg),
            "--min-age", f"{days}d",
            "--include", pat,
            "--tpslimit", "10",
            "--rmdirs",
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

    quota_report = None
    if quota_bytes:
        from .quota import enforce_quota, print_report

        _section("Remote quota")
        try:
            quota_report = enforce_quota(
                remote_path, provider_args(target.provider, cfg), quota=quota_bytes,
                target_pct=cfg.push_quota_target, keep=cfg.push_quota_keep, dry_run=dry_run,
            )
            print_report(quota_report)
            if quota_report.over:
                prune_errors.append("quota: still over the limit after pruning")
        except StorageError as exc:
            prune_errors.append(f"quota: {exc}")
            console.print(f"  [yellow]Warning: quota check failed: {exc}[/]")

    if cfg.manifest and not dry_run:
        _refresh_destination_manifest(remote_path, provider_args(target.provider, cfg))

    status = "warn" if prune_errors else "ok"
    if json_enabled():
        _out({
            "command": "sync",
            "status": status,
            "remote": remote_path,
            "backend": target.provider,
            "files": len(files),
            "latest": keep_latest,
            "quota": quota_report.to_dict() if quota_report else None,
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


def resolve_destinations(cfg: Config, remote: str | list[str] | None = None) -> list[str]:
    """
    Where to push: ``--remote`` (repeatable), else ``PUSH_REMOTE``, else ``REMOTE`` / ``S3_*``.

    An entry is an rclone remote (``name:path``) or a local folder. ``|`` separates several
    destinations inside one value, for example ``PUSH_REMOTE=akilimo-bkp:backups|D:/OneDrive/Backups``.
    """
    items = [remote] if isinstance(remote, str) else list(remote or [])
    if not items and cfg.push_remote:
        items = [cfg.push_remote]
    found: list[str] = []
    for item in items:
        for part in item.split("|"):
            part = part.strip()
            if part and part not in found:
                found.append(part)
    if not found and items:
        return [items[0]]  # an explicit but blank value is an error, not a reason to use the default
    if not found:
        fallback = default_remote(cfg)
        if fallback:
            found.append(fallback)
    return found


def run_sync(
        cfg: Config,
        *,
        remote: str | list[str] | None = None,
        show_remotes: bool = False,
        delete_local: bool = True,
        interactive: bool = True,
        **kwargs,
) -> int:
    """Push backups to one or several destinations. Local files are deleted only after every upload succeeded."""
    if show_remotes:
        return _sync_one(cfg, show_remotes=True)
    dests = resolve_destinations(cfg, remote)
    if len(dests) <= 1:
        return _sync_one(
            cfg, remote=dests[0] if dests else None, delete_local=delete_local, interactive=interactive, **kwargs,
        )

    dry_run = bool(kwargs.get("dry_run"))
    if not json_enabled():
        console.print()
        console.print(Panel(
            "[bold cyan]Push to " + str(len(dests)) + " destinations[/]\n" + "\n".join(f"  - {d}" for d in dests),
            expand=False,
        ))
        if interactive:
            console.print("  [dim]Interactive selection is skipped when pushing to several destinations.[/]")

    collected: list[dict] = []
    outcomes: list[tuple[str, int]] = []
    selection: dict = {}
    for dest in dests:
        if not json_enabled():
            _section(f"Destination: {dest}")
        this: dict = {}
        try:
            code = _sync_one(
                cfg, remote=dest, delete_local=False, interactive=False,
                collect=collected if json_enabled() else None, out=this, **kwargs,
            )
        except SystemExit as exc:  # _die inside one destination must not stop the others
            code = exc.code if isinstance(exc.code, int) and exc.code else EXIT_FAILURE
        outcomes.append((dest, code))
        selection = this or selection

    failed = [dest for dest, code in outcomes if code]
    files = selection.get("files") or []
    removed = 0
    if delete_local and files and not dry_run:
        if failed:
            if not json_enabled():
                console.print(f"\n  [yellow]Local files kept: {len(failed)} destination(s) failed.[/]")
        else:
            for f in files:
                if f.exists():
                    f.unlink()
                    removed += 1
            if removed and selection.get("base_dir"):
                _remove_empty_local_dirs(selection["base_dir"])
            if removed and cfg.manifest and selection.get("base_dir"):
                _refresh_local_manifest(selection["base_dir"])
            if not json_enabled():
                console.print(f"\n  [dim]Removed {removed} local file(s) after all uploads succeeded.[/]")

    if json_enabled():
        emit({
            "command": "sync",
            "status": "fail" if failed else "ok",
            "destinations": [{"destination": d, "exit_code": c} for d, c in outcomes],
            "details": collected,
            "removed_locally": removed,
        })
    elif failed:
        console.print(Panel(
            f"[bold red]PUSH FINISHED — {len(failed)} of {len(dests)} destination(s) failed[/]", expand=False,
        ))
    else:
        console.print(Panel(f"[bold green]PUSH COMPLETE — {len(dests)} destinations[/]", expand=False))
    return EXIT_FAILURE if failed else EXIT_OK
