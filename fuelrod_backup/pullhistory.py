"""Remember which remote files were already pulled, so removing the local copy never causes a re-download.

The history lives in the user config directory (not in the backup folder, where ``push`` would upload it)
and is keyed by ``<remote> -> <destination>``. A file counts as already pulled when its relative path and
size match. Entries for files that no longer exist on the remote are dropped, so the file stays small.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path

from .config import user_config_dir

_FINGERPRINTS = "__manifest_fingerprints__"


def default_path() -> Path:
    return user_config_dir() / "pull-history.json"


def history_key(remote_path: str, destination: Path) -> str:
    return f"{remote_path} -> {destination}"


class PullHistory:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or default_path()
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, int]] = {}
        self._fingerprints: dict[str, str] = {}
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                fps = loaded.pop(_FINGERPRINTS, {})
                self._fingerprints = {k: str(v) for k, v in fps.items()} if isinstance(fps, dict) else {}
                self._data = {k: dict(v) for k, v in loaded.items() if isinstance(v, dict)}
        except (OSError, ValueError):
            self._data = {}

    def size(self, key: str, rel: str) -> int | None:
        with self._lock:
            return self._data.get(key, {}).get(rel)

    def fingerprint(self, key: str) -> str | None:
        """Manifest fingerprint of the server at the end of the last complete, unfiltered pull."""
        with self._lock:
            return self._fingerprints.get(key)

    def set_fingerprint(self, key: str, value: str | None) -> None:
        with self._lock:
            if value:
                self._fingerprints[key] = value
            else:
                self._fingerprints.pop(key, None)
            self._save()

    def count(self, key: str) -> int:
        with self._lock:
            return len(self._data.get(key, {}))

    def record(self, key: str, prefix: str, remote_rels: set[str], have: dict[str, int]) -> None:
        """
        Remember *have* (rel path -> size), and forget entries below *prefix* whose file is no longer
        on the remote (*remote_rels* is every file the remote has there). Saved atomically.
        """
        with self._lock:
            entries = self._data.setdefault(key, {})
            lead = f"{prefix}/" if prefix else ""
            for rel in [r for r in entries if r.startswith(lead) and r not in remote_rels]:
                del entries[rel]
            entries.update(have)
            self._save()

    def _save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=self.path.parent, suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump({**self._data, _FINGERPRINTS: self._fingerprints}, fh, indent=1, sort_keys=True)
            os.replace(tmp, self.path)
        except OSError:
            pass  # history is an optimisation; failing to save it must never fail a pull
