"""Remote quota monitoring: measure what the backups use on a remote and prune the oldest ones to fit a limit.

Usage is the total size of the files under the remote path (``rclone lsjson``), compared with a quota you
set (``PUSH_QUOTA=50G``). When it is exceeded, the oldest files are deleted first until usage drops to
``PUSH_QUOTA_TARGET`` percent of the quota. The newest ``PUSH_QUOTA_KEEP`` backups of every database
(with their manifests, and the full base of any kept incremental) are never touched, so a quota that is
too small stops pruning and reports a failure instead of deleting your only copy.
"""

from __future__ import annotations

import json
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table

from .fileutil import human_size
from .output import json_enabled, register
from .storage import StorageError, _is_artifact, _run_rclone, select_latest

console = register(Console())

_UNITS = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
_SIZE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGT]?)(?:I?B)?\s*$", re.IGNORECASE)
_FRACTION = re.compile(r"(\.\d{6})\d+")


def parse_size(text: str) -> int:
    """``50G`` / ``500 MB`` / ``1.5T`` / ``1024`` -> bytes (1024-based). Raises ValueError."""
    m = _SIZE.match(str(text))
    if not m:
        raise ValueError(f"invalid size {text!r}; use e.g. 500M, 50G or 1T")
    return int(float(m.group(1)) * _UNITS[m.group(2).upper()])


@dataclass(frozen=True)
class RemoteFile:
    path: str  # relative to the remote path, forward slashes
    size: int
    mtime: float

    @property
    def parent(self) -> str:
        return self.path.rpartition("/")[0]

    @property
    def name(self) -> str:
        return self.path.rpartition("/")[2]


@dataclass
class QuotaReport:
    remote: str
    quota: int
    target: int
    usage: int
    files: int
    deleted: list[RemoteFile] = field(default_factory=list)
    protected: int = 0
    dry_run: bool = False
    provider: dict | None = None
    listing: list = field(default_factory=list, repr=False)
    pruned: bool = False  # pruning was requested (a report-only run is not)

    @property
    def freed(self) -> int:
        return sum(f.size for f in self.deleted)

    @property
    def after(self) -> int:
        return self.usage - self.freed

    @property
    def over(self) -> bool:
        """Still above the quota once pruning is accounted for."""
        return self.after > self.quota

    @property
    def ok(self) -> bool:
        return not self.over

    @property
    def percent(self) -> float:
        return 100.0 * self.usage / self.quota if self.quota else 0.0

    def to_dict(self) -> dict:
        return {
            "remote": self.remote,
            "quota_bytes": self.quota,
            "target_bytes": self.target,
            "usage_bytes": self.usage,
            "usage_percent": round(self.percent, 1),
            "files": self.files,
            "deleted": [f.path for f in self.deleted],
            "freed_bytes": self.freed,
            "after_bytes": self.after,
            "protected_files": self.protected,
            "over_quota": self.over,
            "dry_run": self.dry_run,
            "provider": self.provider,
        }


def list_remote(remote_path: str, extra: list[str]) -> list[RemoteFile]:
    """Every file under *remote_path* with size and modification time."""
    code, out, err = _run_rclone(["lsjson", "-R", "--files-only", "--no-mimetype", remote_path, *extra], timeout=900)
    if code != 0:
        lines = [ln for ln in err.strip().splitlines() if ln.strip()]
        raise StorageError(lines[-1] if lines else f"rclone lsjson exited {code}")
    try:
        items = json.loads(out or "[]")
    except json.JSONDecodeError as exc:
        raise StorageError(f"could not read the remote listing: {exc}") from exc
    files = []
    for item in items:
        stamp = _FRACTION.sub(r"\1", str(item.get("ModTime", "")).replace("Z", "+00:00"))
        try:
            mtime = datetime.fromisoformat(stamp).timestamp()
        except ValueError:
            mtime = 0.0
        files.append(RemoteFile(path=item["Path"], size=int(item.get("Size", 0)), mtime=mtime))
    return files


def provider_about(remote_path: str, extra: list[str]) -> dict | None:
    """What the provider itself reports (total/used/free), when it supports ``rclone about``."""
    code, out, _err = _run_rclone(["about", "--json", remote_path, *extra], timeout=60)
    if code != 0:
        return None
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return None
    return {k: v for k, v in data.items() if k in ("total", "used", "free") and isinstance(v, int)} or None


def plan_prune(
    files: list[RemoteFile], quota: int, target_pct: int, keep: int,
) -> tuple[list[RemoteFile], int, int]:
    """
    Decide what to delete. Returns (files to delete oldest-first, protected count, target bytes).

    Nothing is deleted while usage is within the quota. Once it is over, files are removed oldest
    first until usage is at or below ``quota * target_pct / 100``.
    """
    usage = sum(f.size for f in files)
    target = int(quota * max(1, min(100, target_pct)) / 100)
    if usage <= quota:
        return [], 0, target

    protected = set(select_latest(files, max(1, keep)))
    rest = [f for f in files if f not in protected]

    # A backup is its dump plus its manifest: they are deleted together, never one without the other.
    manifests = [f for f in rest if f.name.startswith("manifest_") and f.name.endswith(".txt")]
    taken: set[RemoteFile] = set()
    units: list[list[RemoteFile]] = []
    for dump in (f for f in rest if _is_artifact(f)):
        mine = [m for m in manifests if m.parent == dump.parent and m.name[len("manifest_"):-len(".txt")] in dump.name]
        taken.update(mine)
        units.append([dump, *mine])
    units.extend([f] for f in rest if f not in taken and not _is_artifact(f))
    units.sort(key=lambda unit: (unit[0].mtime, unit[0].path))

    doomed: list[RemoteFile] = []
    current = usage
    for unit in units:
        if current <= target:
            break
        doomed.extend(unit)
        current -= sum(f.size for f in unit)
    return doomed, len(protected), target


def _delete(
    remote_path: str, extra: list[str], files: list[RemoteFile], *, rmdirs: bool = True, dry_run: bool = False,
) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as fh:
        fh.write("\n".join(f.path for f in files) + "\n")
        listing = Path(fh.name)
    try:
        args = ["delete", remote_path, "--files-from-raw", str(listing), *extra]
        if rmdirs:
            args.append("--rmdirs")
        if dry_run:
            args.append("--dry-run")
        code, _out, err = _run_rclone(args, timeout=1800)
    finally:
        listing.unlink(missing_ok=True)
    if code != 0:
        lines = [ln for ln in err.strip().splitlines() if ln.strip()]
        raise StorageError(f"deleting files failed: {lines[-1] if lines else f'exit {code}'}")


def _refresh_manifest_after_prune(remote_path: str, extra: list[str]) -> None:
    """Keep the destination's manifest.json truthful after files were removed (only if it already has one)."""
    from .manifest import read_remote, refresh_remote

    try:
        if read_remote(remote_path, extra) is not None:
            refresh_remote(remote_path, extra)
    except StorageError:
        pass  # a stale manifest is detected by readers (fingerprint) and falls back to listing


def enforce_quota(
    remote_path: str,
    extra: list[str],
    *,
    quota: int,
    target_pct: int = 90,
    keep: int = 1,
    prune: bool = True,
    dry_run: bool = False,
) -> QuotaReport:
    """Measure usage and, when *prune* is set and the quota is exceeded, delete the oldest backups."""
    files = list_remote(remote_path, extra)
    doomed, protected, target = plan_prune(files, quota, target_pct, keep) if prune else ([], 0, 0)
    report = QuotaReport(
        remote=remote_path, quota=quota, usage=sum(f.size for f in files), files=len(files),
        target=int(quota * max(1, min(100, target_pct)) / 100), protected=protected, dry_run=dry_run,
        listing=files, pruned=prune,
    )
    if doomed:
        if not dry_run:
            _delete(remote_path, extra, doomed)
            _refresh_manifest_after_prune(remote_path, extra)
        report.deleted = doomed
    report.provider = provider_about(remote_path, extra)
    return report


def by_database(files: list[RemoteFile]) -> list[tuple[str, int, int]]:
    """(folder, file count, bytes) per top two path levels, biggest first."""
    totals: dict[str, list[int]] = {}
    for f in files:
        key = "/".join(f.path.split("/")[:2]) if f.path.count("/") >= 2 else (f.parent or "(root)")
        entry = totals.setdefault(key, [0, 0])
        entry[0] += 1
        entry[1] += f.size
    return sorted(((k, v[0], v[1]) for k, v in totals.items()), key=lambda row: -row[2])


def print_report(report: QuotaReport, *, table: bool = False) -> None:
    """Human-readable summary (silent in JSON mode)."""
    if json_enabled():
        return
    console.print()
    color = "red" if report.over else ("yellow" if report.percent >= 80 else "green")
    console.print(
        f"  Remote usage : [bold]{human_size(report.usage)}[/] of {human_size(report.quota)} quota "
        f"([{color}]{report.percent:.0f}%[/]) in {report.files} file(s)"
    )
    if report.provider:
        prov = report.provider
        parts = [f"{k} {human_size(v)}" for k, v in prov.items()]
        console.print(f"  Provider says: {', '.join(parts)}")
    if table and report.listing:
        grid = Table(show_header=True, header_style="bold")
        grid.add_column("Database folder")
        grid.add_column("Files", justify="right")
        grid.add_column("Size", justify="right")
        for folder, count, size in by_database(report.listing)[:15]:
            grid.add_row(folder, str(count), human_size(size))
        console.print(grid)
    if report.deleted:
        verb = "Would delete" if report.dry_run else "Deleted"
        console.print(
            f"  [yellow]{verb} {len(report.deleted)} old file(s)[/], freeing {human_size(report.freed)} "
            f"-> {human_size(report.after)} ({100 * report.after / report.quota:.0f}% of quota)"
        )
    if report.over and report.pruned:
        console.print(
            f"  [bold red]Still over quota by {human_size(report.after - report.quota)}[/] after pruning "
            f"({report.protected} protected file(s) are never deleted). Raise the quota or lower PUSH_QUOTA_KEEP."
        )
    elif report.over:
        console.print(f"  [bold red]Over quota by {human_size(report.after - report.quota)}[/].")
