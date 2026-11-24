"""Pull backups from an rclone remote down to local storage, one parallel job per engine/database.

The remote layout mirrors BASE_DIR (``<remote>/<engine>/<database>/<files>``), which is what
``sync`` uploads. Each ``<engine>/<database>`` folder is copied by its own rclone process and a
thread pool runs several of them at once, so one slow or failing database never blocks the rest.

Nothing is ever deleted locally. A file is downloaded only when it is neither already in the local
folder (same size) nor in the pull history (``pullhistory``), so removing a local copy — for example
after ``push`` moved it elsewhere — does not cause a re-download. ``--delete-remote`` removes server files
afterwards, but only files that are verified to be in the local folder with the same size.
"""

from __future__ import annotations

import re
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from .config import Config, DbType
from .fileutil import human_size
from .manifest import MANIFEST_NAME, read_remote, refresh_remote
from .notify import EXIT_FAILURE, EXIT_OK
from .output import emit, error, json_enabled, register, warn
from .pullhistory import PullHistory, history_key
from .quota import RemoteFile, _delete, list_remote
from .storage import (
    StorageError,
    _die,
    default_remote,
    log_line,
    parse_remote,
    provider_args,
    rclone_available,
    run_rclone_process,
    select_latest,
    verbose_level,
)

console = register(Console())

JOB_TIMEOUT = 6 * 3600  # seconds per rclone process
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_. \-]*$")


@dataclass
class PullJob:
    """One ``<engine>/<database>`` folder (or the whole remote when the layout is flat)."""

    source: str
    dest: Path
    label: str
    files: list[RemoteFile] | None = None  # from the manifest; None = list the folder on the server

    @property
    def prefix(self) -> str:
        """Path of this job below the remote root (empty for a whole-remote job)."""
        return "" if self.label == "(all)" else self.label


@dataclass
class PullResult:
    label: str
    status: str  # ok | fail
    files: int = 0  # downloaded (or, in a dry run, that would be)
    skipped: int = 0  # already local or already pulled before
    excluded: int = 0  # ignored because they match an exclude pattern
    deleted: int = 0  # removed from the server
    seconds: float = 0.0
    error: str = ""


@dataclass
class PullContext:
    extra: list[str]
    args: list[str]
    include_patterns: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)
    since: int | None = None
    overwrite: bool = False
    ignore_history: bool = False
    history: PullHistory | None = None
    history_key: str = ""
    delete_remote: bool = False
    keep_remote: int = 0
    verify: bool = False
    dry_run: bool = False


def _lsf_dirs(remote_path: str, extra: list[str]) -> list[str]:
    """Immediate sub-directory names under *remote_path*."""
    try:
        result = run_rclone_process(["lsf", "--dirs-only", remote_path, *extra], 120)
    except FileNotFoundError as exc:
        raise StorageError("rclone not found in PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise StorageError(f"listing {remote_path} timed out") from exc
    if result.returncode != 0:
        msg = result.stderr.decode(errors="replace").strip()
        raise StorageError(msg.splitlines()[-1] if msg else f"rclone lsf exited {result.returncode}")
    return [ln.strip().rstrip("/") for ln in result.stdout.decode(errors="replace").splitlines() if ln.strip()]


def _safe(name: str) -> bool:
    """Remote-supplied folder names become local paths, so reject anything path-like."""
    return bool(_SAFE_NAME.match(name)) and name not in (".", "..")


def discover_jobs(
    remote_path: str,
    dest: Path,
    extra: list[str],
    *,
    engines: list[str] | None = None,
    databases: list[str] | None = None,
) -> list[PullJob]:
    """Build one job per ``<engine>/<database>``; fall back to a single whole-remote job."""
    base = remote_path if remote_path.endswith(":") else remote_path.rstrip("/") + "/"
    top = _lsf_dirs(remote_path, extra)
    if not top:
        return [PullJob(source=remote_path, dest=dest, label="(all)")]

    if not engines:
        # Only engine folders are backups. Pointing at e.g. a home directory must not download everything in it.
        known = [name for name in top if name in {e.value for e in DbType}]
        if not known:
            shown = ", ".join(sorted(top)[:8]) + (" …" if len(top) > 8 else "")
            raise StorageError(
                "no postgres/mariadb/mssql folders here (found: " + shown + "). Point --remote at the folder that "
                "holds them (for example akilimo-bkp:db-backup), or pass --engine NAME to pull specific folders."
            )
        engines = known

    jobs: list[PullJob] = []
    for engine in sorted(top):
        if engines and engine not in engines:
            continue
        if not _safe(engine):
            warn(f"skipping unsafe remote folder name {engine!r}")
            continue
        dbs = _lsf_dirs(f"{base}{engine}", extra)
        if not dbs:  # engine folder holds files directly
            jobs.append(PullJob(f"{base}{engine}", dest / engine, engine))
            continue
        for db in sorted(dbs):
            if databases and db not in databases:
                continue
            if not _safe(db):
                warn(f"skipping unsafe remote folder name {engine}/{db!r}")
                continue
            jobs.append(PullJob(f"{base}{engine}/{db}", dest / engine / db, f"{engine}/{db}"))
    return jobs


def _safe_rel(path: str) -> bool:
    parts = path.split("/")
    return bool(path) and not path.startswith("/") and ".." not in parts and not any(p == "" for p in parts)


def _matches(patterns: list[str], f: RemoteFile, prefix: str) -> bool:
    """A pattern may name the file (``*.tmp``), its path in the database folder, or the full ``engine/db/file`` path."""
    full = f"{prefix}/{f.path}".lstrip("/")
    return any(fnmatchcase(f.name, p) or fnmatchcase(f.path, p) or fnmatchcase(full, p) for p in patterns)


def _wanted(files: list[RemoteFile], ctx: PullContext, prefix: str = "") -> tuple[list[RemoteFile], int]:
    """The files this run is interested in, and how many were ignored by --exclude."""
    cutoff = time.time() - ctx.since * 86400 if ctx.since is not None else None
    chosen = []
    excluded = 0
    for f in files:
        if not _safe_rel(f.path):
            warn(f"skipping unsafe remote path {f.path!r}")
            continue
        if cutoff is not None and f.mtime < cutoff:
            continue
        if ctx.include_patterns and not _matches(ctx.include_patterns, f, prefix):
            continue
        if ctx.exclude_patterns and _matches(ctx.exclude_patterns, f, prefix):
            excluded += 1
            if verbose_level() >= 2:
                log_line(f"[{prefix or '(all)'}] ignore {f.path}: matches an exclude pattern")
            continue
        chosen.append(f)
    return chosen, excluded


def _local_has(job: PullJob, f: RemoteFile) -> bool:
    local = job.dest / f.path
    try:
        return local.is_file() and local.stat().st_size == f.size
    except OSError:
        return False


def _write_list(paths: list[str]) -> Path:
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as fh:
        fh.write("\n".join(paths) + "\n")
        return Path(fh.name)


def _rclone(args: list[str], timeout: int, label: str = "") -> subprocess.CompletedProcess:
    return run_rclone_process(args, timeout, label)


def _hash_verified(job: PullJob, files: list[RemoteFile], ctx: PullContext) -> list[RemoteFile]:
    """Keep only the files rclone confirms identical on both sides (checksums where the remote has them)."""
    listing = _write_list([f.path for f in files])
    try:
        result = _rclone(
            ["check", job.source, str(job.dest), "--one-way", "--files-from-raw", str(listing),
             "--combined", "-", *ctx.extra], JOB_TIMEOUT, job.label,
        )
    finally:
        listing.unlink(missing_ok=True)
    same = {ln[2:] for ln in result.stdout.decode(errors="replace").splitlines() if ln.startswith("= ")}
    return [f for f in files if f.path in same]


def jobs_from_manifest(
    files: list[RemoteFile],
    remote_path: str,
    dest: Path,
    *,
    engines: list[str] | None = None,
    databases: list[str] | None = None,
) -> list[PullJob] | None:
    """
    One job per ``<engine>/<database>`` taken from the manifest — no traversal of the server at all.

    Returns None when the manifest does not have the expected ``engine/database/file`` layout, so the
    caller falls back to listing the folders.
    """
    base = remote_path if remote_path.endswith(":") else remote_path.rstrip("/") + "/"
    groups: dict[tuple[str, str], list[RemoteFile]] = {}
    for f in files:
        parts = f.path.split("/")
        if len(parts) < 3:
            return None
        groups.setdefault((parts[0], parts[1]), []).append(
            RemoteFile(path="/".join(parts[2:]), size=f.size, mtime=f.mtime),
        )
    if not engines:
        known = {e.value for e in DbType}
        if not any(engine in known for engine, _db in groups):
            return None  # not a backup tree: let the listing path explain what it found
        engines = sorted({engine for engine, _db in groups if engine in known})
    jobs: list[PullJob] = []
    for (engine, db), rows in sorted(groups.items()):
        if engine not in engines or (databases and db not in databases):
            continue
        if not (_safe(engine) and _safe(db)):
            warn(f"skipping unsafe folder name {engine}/{db}")
            continue
        jobs.append(PullJob(f"{base}{engine}/{db}", dest / engine / db, f"{engine}/{db}", files=rows))
    return jobs


def _run_job(job: PullJob, ctx: PullContext) -> PullResult:
    started = time.monotonic()

    def failed(message: str) -> PullResult:
        return PullResult(job.label, "fail", seconds=time.monotonic() - started, error=message)

    if job.files is not None:
        remote_files = job.files
    else:
        try:
            remote_files = list_remote(job.source, ctx.extra)
        except StorageError as exc:
            return failed(str(exc))
    wanted, excluded = _wanted(remote_files, ctx, job.prefix)

    to_fetch: list[RemoteFile] = []
    have: dict[str, int] = {}
    level = verbose_level()
    for f in wanted:
        rel = f"{job.prefix}/{f.path}".lstrip("/")
        in_history = (
            ctx.history is not None and not ctx.ignore_history and ctx.history.size(ctx.history_key, rel) == f.size
        )
        on_disk = _local_has(job, f)
        if ctx.overwrite or not (in_history or on_disk):
            to_fetch.append(f)
            if level >= 2:
                log_line(f"[{job.label}] download {f.path} ({human_size(f.size)})")
        else:
            have[f.path] = f.size
            if level >= 2:
                why = "already in the local folder" if on_disk else "pulled before"
                log_line(f"[{job.label}] skip {f.path}: {why}")
    skipped = len(have)
    if level:
        log_line(
            f"[{job.label}] {len(remote_files)} file(s) on the server, {len(wanted)} selected, "
            f"{len(to_fetch)} to download ({human_size(sum(f.size for f in to_fetch))}), {skipped} already have"
            + (f", {excluded} excluded" if excluded else "")
        )

    if to_fetch:
        listing = _write_list([f.path for f in to_fetch])
        try:
            result = _rclone(
                ["copy", job.source, str(job.dest), "--files-from-raw", str(listing), *ctx.args], JOB_TIMEOUT,
                job.label,
            )
        except FileNotFoundError:
            return failed("rclone not found in PATH")
        except subprocess.TimeoutExpired:
            return failed(f"timed out after {JOB_TIMEOUT}s")
        finally:
            listing.unlink(missing_ok=True)
        if result.returncode != 0:
            if job.files is not None:
                # The manifest may be stale (files removed on the server since it was written): list for real.
                log_line(f"[{job.label}] download failed using the manifest; listing the folder instead") if (
                    verbose_level()
                ) else None
                job.files = None
                return _run_job(job, ctx)
            lines = [ln for ln in result.stderr.decode(errors="replace").strip().splitlines() if ln.strip()]
            return failed(lines[-1] if lines else f"rclone exited {result.returncode}")
        if not ctx.dry_run:
            have.update({f.path: f.size for f in to_fetch})

    if ctx.history is not None and not ctx.dry_run:
        prefix = job.prefix
        rels = {f"{prefix}/{f.path}".lstrip("/") for f in remote_files}
        ctx.history.record(
            ctx.history_key, prefix, rels, {f"{prefix}/{p}".lstrip("/"): size for p, size in have.items()},
        )

    deleted = 0
    if ctx.delete_remote:
        try:
            doomed = [f for f in wanted if _local_has(job, f)]
            if ctx.keep_remote > 0:
                protected = set(select_latest(remote_files, ctx.keep_remote))
                doomed = [f for f in doomed if f not in protected]
            if ctx.verify and doomed and not ctx.dry_run:
                doomed = _hash_verified(job, doomed, ctx)
            if level:
                log_line(f"[{job.label}] {len(doomed)} file(s) to delete on the server")
            if doomed:
                _delete(job.source, ctx.extra, doomed, rmdirs=False, dry_run=ctx.dry_run)
            deleted = len(doomed)
        except (StorageError, subprocess.TimeoutExpired) as exc:
            return PullResult(
                job.label, "fail", files=len(to_fetch), skipped=skipped, seconds=time.monotonic() - started,
                error=f"downloaded, but deleting on the server failed: {exc}",
            )

    return PullResult(
        job.label, "ok", files=len(to_fetch), skipped=skipped, deleted=deleted, excluded=excluded,
        seconds=time.monotonic() - started,
    )


def build_args(
    cfg: Config,
    provider: str,
    *,
    transfers: int,
    checkers: int,
    bwlimit: str | None,
    dry_run: bool,
) -> list[str]:
    """rclone flags shared by every job (which files to fetch is decided per job, from the listing)."""
    args = [
        *provider_args(provider, cfg),
        "--transfers", str(transfers),
        "--checkers", str(checkers),
        "--retries", "3",
        "--low-level-retries", "10",
        "--contimeout", "60s",
        "--timeout", "300s",
    ]
    if bwlimit:
        args += ["--bwlimit", bwlimit]
    if dry_run:
        args.append("--dry-run")
    return args


def _exclude_list(cfg: Config, patterns: list[str] | None) -> list[str]:
    """--exclude values plus PULL_EXCLUDE from the config (``|`` separated), without duplicates."""
    merged: list[str] = []
    for raw in [*(patterns or []), *cfg.pull_exclude.split("|")]:
        pat = raw.strip()
        if pat and pat not in merged:
            merged.append(pat)
    return merged


def _nothing_new(remote_path: str, destination: Path, fp: str, count: int) -> int:
    """The server's manifest is unchanged since the last complete pull: finish without touching its folders."""
    if json_enabled():
        emit({
            "command": "pull", "status": "ok", "remote": remote_path, "destination": str(destination),
            "dry_run": False, "files": 0, "skipped": count, "deleted_on_server": 0, "jobs": [],
            "manifest": {"fingerprint": fp, "unchanged": True},
        })
    else:
        console.print(Panel(
            f"[bold green]NOTHING NEW[/] — {MANIFEST_NAME} on the server is unchanged ({count} file(s)).\n"
            "[dim]No folders were listed. Use --no-manifest to check the folders anyway.[/]",
            expand=False,
        ))
    return EXIT_OK


def run_pull(
    cfg: Config,
    *,
    remote: str | None = None,
    dest: str | Path | None = None,
    engines: list[str] | None = None,
    databases: list[str] | None = None,
    include_patterns: list[str] | None = None,
    since: int | None = None,
    workers: int = 4,
    transfers: int = 4,
    checkers: int = 8,
    bwlimit: str | None = None,
    dry_run: bool = False,
    overwrite: bool = False,
    ignore_history: bool = False,
    delete_remote: bool | None = None,
    keep_remote: int | None = None,
    verify: bool = False,
    yes: bool = False,
    use_manifest: bool = True,
    exclude_patterns: list[str] | None = None,
) -> int:
    """Download backups from *remote* into *dest* (default BASE_DIR). Returns a process exit code."""
    if not rclone_available():
        _die("rclone not found in PATH. Install it from https://rclone.org/install/")
    if not remote and cfg.pull_remote:
        remote = cfg.pull_remote  # PULL_REMOTE: a configured rclone remote
    spec = remote or default_remote(cfg)
    if not spec:
        _die("No remote configured. Pass --remote or set PULL_REMOTE (see `fuelrod-backup pull-init`).")
    try:
        target = parse_remote(spec)
    except ValueError as exc:
        _die(str(exc))
    if not (dest or cfg.base_dir):
        _die("No destination. Pass --dest or set BASE_DIR.")
    destination = Path(dest or cfg.base_dir)
    if workers < 1 or transfers < 1 or checkers < 1:
        _die("--parallel, --transfers and --checkers must be at least 1.")

    remote_path = target.location
    extra = provider_args(target.provider, cfg)
    delete_remote = cfg.pull_delete_remote if delete_remote is None else delete_remote
    keep_remote = cfg.pull_keep_remote if keep_remote is None else max(0, keep_remote)
    if delete_remote and not dry_run and not yes:
        _die("Deleting server files after pulling needs confirmation: add --yes (or use --dry-run to preview).")

    key = history_key(remote_path, destination)
    history = PullHistory()
    exclusions = _exclude_list(cfg, exclude_patterns)
    unfiltered = not (engines or databases or include_patterns or exclusions or since is not None)

    jobs: list[PullJob] | None = None
    manifest_fp: str | None = None
    if use_manifest and not overwrite:
        found = read_remote(remote_path, extra)
        if found is not None:
            manifest_files, manifest_fp = found
            if unfiltered and not ignore_history and not delete_remote and history.fingerprint(key) == manifest_fp:
                return _nothing_new(remote_path, destination, manifest_fp, len(manifest_files))
            jobs = jobs_from_manifest(
                manifest_files, remote_path, destination, engines=engines, databases=databases,
            )
            if jobs is None:
                manifest_fp = None  # unusual layout: traverse as before
            elif verbose_level():
                log_line(f"read {MANIFEST_NAME} from the server: {len(manifest_files)} file(s), no folders listed")
    if jobs is None:
        try:
            jobs = discover_jobs(remote_path, destination, extra, engines=engines, databases=databases)
        except StorageError as exc:
            _die(f"Could not list {remote_path}: {exc}")
    if not jobs:
        _die("Nothing to pull: no remote folders match the given --engine/--db filters.")

    if not dry_run:
        destination.mkdir(parents=True, exist_ok=True)
    ctx = PullContext(
        extra=extra,
        args=build_args(
            cfg, target.provider, transfers=transfers, checkers=checkers, bwlimit=bwlimit, dry_run=dry_run,
        ),
        include_patterns=list(include_patterns or []),
        exclude_patterns=exclusions,
        since=since,
        overwrite=overwrite,
        ignore_history=ignore_history,
        history=history,
        history_key=key,
        delete_remote=delete_remote,
        keep_remote=keep_remote,
        verify=verify,
        dry_run=dry_run,
    )

    workers = min(workers, len(jobs))
    if not json_enabled():
        console.print(Panel(
            f"[bold cyan]PULL[/] {remote_path} → {destination}\n"
            f"[dim]{len(jobs)} job(s), {workers} in parallel, {transfers} transfer(s) each"
            f"{' — DRY RUN' if dry_run else ''}[/]\n"
            + (f"[yellow]Server files are deleted after a verified pull"
               f"{f' (newest {keep_remote} per database kept)' if keep_remote else ''}.[/]" if delete_remote else
               "[dim]Files already pulled before are skipped, even if the local copy was removed.[/]"),
            expand=False,
        ))

    results: list[PullResult] = []
    with Progress(
        SpinnerColumn(), TextColumn("{task.description}"), BarColumn(), TimeElapsedColumn(),
        console=console, transient=True,
    ) as progress:
        overall = progress.add_task(f"[cyan]0/{len(jobs)} done[/]", total=len(jobs))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_run_job, job, ctx): job for job in jobs}
            for fut in as_completed(futures):
                res = fut.result()
                results.append(res)
                icon = "[green]✓[/]" if res.status == "ok" else "[red]✗[/]"
                detail = (
                    f"{res.files} new · {res.skipped} already have"
                    + (f" · {res.excluded} excluded" if res.excluded else "")
                    + (f" · {res.deleted} deleted on server" if delete_remote else "")
                    if res.status == "ok" else res.error
                )
                progress.console.print(f"  {icon} {res.label}  [dim]{detail} · {res.seconds:.0f}s[/]")
                progress.update(overall, advance=1, description=f"[cyan]{len(results)}/{len(jobs)} done[/]")

    results.sort(key=lambda r: r.label)
    failed = [r for r in results if r.status != "ok"]
    total_files = sum(r.files for r in results)
    total_skipped = sum(r.skipped for r in results)
    total_deleted = sum(r.deleted for r in results)
    total_excluded = sum(r.excluded for r in results)

    if not dry_run and use_manifest and manifest_fp is not None:
        if total_deleted:  # the server changed: publish a fresh manifest and remember that state instead
            try:
                manifest_fp = refresh_remote(remote_path, extra)["fingerprint"]
            except StorageError as exc:
                warn(f"could not update the manifest on the server: {exc}")
                manifest_fp = None
        # Only a complete, unfiltered run proves everything in the manifest has been handled.
        history.set_fingerprint(key, manifest_fp if unfiltered and not failed else None)

    if json_enabled():
        emit({
            "command": "pull",
            "status": "fail" if failed else "ok",
            "remote": remote_path,
            "destination": str(destination),
            "dry_run": dry_run,
            "files": total_files,
            "skipped": total_skipped,
            "deleted_on_server": total_deleted,
            "excluded": total_excluded,
            "jobs": [
                {"job": r.label, "status": r.status, "files": r.files, "skipped": r.skipped,
                 "excluded": r.excluded, "deleted": r.deleted, "seconds": round(r.seconds, 1), "error": r.error}
                for r in results
            ],
        })
    elif failed:
        console.print(Panel(
            f"[bold red]PULL FINISHED — {len(failed)} of {len(results)} job(s) failed[/]", expand=False,
        ))
    else:
        extra_info = f", {total_deleted} deleted on the server" if delete_remote else ""
        console.print(Panel(
            f"[bold green]PULL COMPLETE — {total_files} new, {total_skipped} already had{extra_info}[/]", expand=False,
        ))

    if failed:
        detail = "; ".join(f"{r.label}: {r.error}" for r in failed)
        error(f"{len(failed)} pull job(s) failed: {detail}")
        return EXIT_FAILURE
    return EXIT_OK
