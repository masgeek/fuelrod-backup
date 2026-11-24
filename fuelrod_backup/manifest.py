"""A machine-readable index (``manifest.json``) of every backup in a backup tree.

Without it, anything that wants to know "what is there?" has to walk the folders — on a remote
(SFTP, S3, Drive) that is one listing request per folder, even when nothing changed. The manifest
is one small file at the root of the tree::

    {"version": 1, "generated": 1766000000, "fingerprint": "…", "files": [
        {"path": "postgres/app/app_20261124_031500.dump.gz", "size": 559771488, "mtime": 1766000000}, …]}

It is rewritten whenever the tree changes (after a backup, a retention cleanup, a push cleanup, a
quota prune), and ``push`` refreshes the copy on the destination. ``pull`` reads that single file
instead of traversing, and when its ``fingerprint`` is unchanged since the last pull it does not
touch the server's folders at all.

The fingerprint covers path, size and modification time (to the second), so it is identical for the
same files wherever they are listed from.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from collections.abc import Iterable
from pathlib import Path

from .quota import RemoteFile, list_remote
from .storage import StorageError, _run_rclone

MANIFEST_NAME = "manifest.json"
VERSION = 1


def fingerprint(files: Iterable[RemoteFile]) -> str:
    digest = hashlib.sha256()
    for f in sorted(files, key=lambda x: x.path):
        digest.update(f"{f.path}|{f.size}|{int(f.mtime)}\n".encode())
    return digest.hexdigest()[:24]


def build(files: Iterable[RemoteFile]) -> dict:
    """Manifest document for *files* (relative paths, forward slashes)."""
    rows = sorted((f for f in files if f.path != MANIFEST_NAME), key=lambda f: f.path)
    return {
        "version": VERSION,
        "generated": int(time.time()),
        "fingerprint": fingerprint(rows),
        "files": [{"path": f.path, "size": f.size, "mtime": int(f.mtime)} for f in rows],
    }


def parse(document: object) -> list[RemoteFile] | None:
    """The files a manifest lists, or None when it is not a usable manifest."""
    if not isinstance(document, dict) or document.get("version") != VERSION:
        return None
    rows = document.get("files")
    if not isinstance(rows, list):
        return None
    files: list[RemoteFile] = []
    try:
        for row in rows:
            files.append(RemoteFile(path=str(row["path"]), size=int(row["size"]), mtime=float(row["mtime"])))
    except (KeyError, TypeError, ValueError):
        return None
    if document.get("fingerprint") != fingerprint(files):
        return None  # truncated or edited by hand — do not trust it
    return files


# ───────────────────────── local tree ─────────────────────────

def scan_local(base_dir: Path) -> list[RemoteFile]:
    """Every backup file under *base_dir* (hidden files and the manifest itself are not backups)."""
    base = Path(base_dir)
    found: list[RemoteFile] = []
    for root, dirs, names in os.walk(base):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for name in names:
            if name.startswith(".") or (name == MANIFEST_NAME and Path(root) == base):
                continue
            full = Path(root) / name
            try:
                st = full.stat()
            except OSError:
                continue
            found.append(RemoteFile(path=full.relative_to(base).as_posix(), size=st.st_size, mtime=st.st_mtime))
    return found


def _atomic_write(path: Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(document, fh, indent=1)
    os.replace(tmp, path)


def write_local(base_dir: Path) -> Path:
    """Rewrite ``<base_dir>/manifest.json`` from what is on disk. Cheap: one local walk."""
    target = Path(base_dir) / MANIFEST_NAME
    _atomic_write(target, build(scan_local(base_dir)))
    return target


def read_local(base_dir: Path) -> list[RemoteFile] | None:
    try:
        return parse(json.loads((Path(base_dir) / MANIFEST_NAME).read_text(encoding="utf-8")))
    except (OSError, ValueError):
        return None


# ───────────────────────── remote / destination ─────────────────────────

def _join(remote_path: str, name: str) -> str:
    return remote_path + name if remote_path.endswith(":") else remote_path.rstrip("/") + "/" + name


def read_remote(remote_path: str, extra: list[str]) -> tuple[list[RemoteFile], str] | None:
    """(files, fingerprint) from the manifest at *remote_path*, or None when absent / unusable."""
    code, out, _err = _run_rclone(["cat", _join(remote_path, MANIFEST_NAME), *extra], timeout=120)
    if code != 0 or not out.strip():
        return None
    try:
        document = json.loads(out)
    except ValueError:
        return None
    files = parse(document)
    return (files, document["fingerprint"]) if files is not None else None


def upload_remote(remote_path: str, extra: list[str], document: dict) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as fh:
        json.dump(document, fh, indent=1)
        tmp = Path(fh.name)
    try:
        code, _out, err = _run_rclone(
            ["copyto", str(tmp), _join(remote_path, MANIFEST_NAME), *extra], timeout=300,
        )
    finally:
        tmp.unlink(missing_ok=True)
    if code != 0:
        lines = [ln for ln in err.strip().splitlines() if ln.strip()]
        raise StorageError(f"could not write the manifest: {lines[-1] if lines else f'exit {code}'}")


def refresh_remote(remote_path: str, extra: list[str]) -> dict:
    """List *remote_path* once and publish an up-to-date manifest there. Returns the document."""
    files = [f for f in list_remote(remote_path, extra) if f.path != MANIFEST_NAME]
    document = build(files)
    upload_remote(remote_path, extra, document)
    return document
