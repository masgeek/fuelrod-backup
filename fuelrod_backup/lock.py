"""Per-engine run lock so backups and restores never overlap."""

from __future__ import annotations

import json
import os
import socket
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

STALE_AFTER = 24 * 3600  # seconds; a lock this old is assumed abandoned


class LockError(RuntimeError):
    """Another run holds the lock."""


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        code = ctypes.c_ulong()
        ok = ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
        ctypes.windll.kernel32.CloseHandle(handle)
        return bool(ok) and code.value == 259  # STILL_ACTIVE
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _read(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _is_stale(info: dict, path: Path) -> bool:
    same_host = info.get("host") == socket.gethostname()
    if same_host and info.get("pid") and not _pid_alive(int(info["pid"])):
        return True
    try:
        return time.time() - path.stat().st_mtime > STALE_AFTER
    except OSError:
        return True


@contextmanager
def run_lock(base_dir: str | Path, name: str, operation: str) -> Iterator[Path]:
    """Hold ``<base_dir>/.fuelrod-<name>.lock`` for the duration of the block."""
    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)
    path = base / f".fuelrod-{name}.lock"
    payload = json.dumps({"pid": os.getpid(), "host": socket.gethostname(), "operation": operation, "ts": time.time()})

    for _ in range(2):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            info = _read(path)
            if _is_stale(info, path):
                path.unlink(missing_ok=True)
                continue
            raise LockError(
                f"Another '{info.get('operation', 'run')}' is in progress for '{name}' "
                f"(pid {info.get('pid', '?')} on {info.get('host', '?')}). "
                f"If that is wrong, delete {path}."
            ) from None
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        break
    else:
        raise LockError(f"Could not acquire lock {path}")

    try:
        yield path
    finally:
        path.unlink(missing_ok=True)
