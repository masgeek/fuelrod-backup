"""Backup catalog: enumerate backups per engine/database, and detect suspicious size drops."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import DbType
from .fileutil import human_size

_ARTIFACT_PATTERNS = ("*.dump", "*.dump.gz", "*.sql", "*.sql.gz", "*.zip", "*.bak", "*.age", "*.gpg")


@dataclass
class CatalogEntry:
    engine: str
    database: str
    file: str
    size: int
    mtime: float
    encrypted: bool
    compressed: bool
    incremental: bool

    def to_dict(self) -> dict:
        d = asdict(self)
        d["age_days"] = round((time.time() - self.mtime) / 86400, 2)
        return d


def _artifacts(db_dir: Path) -> list[Path]:
    found: set[Path] = set()
    for pat in _ARTIFACT_PATTERNS:
        found.update(db_dir.glob(pat))
    return sorted(found, key=lambda p: p.stat().st_mtime)


def list_backups(
    base_dir: str | Path,
    engines: list[str] | None = None,
    database: str | None = None,
) -> list[CatalogEntry]:
    """Walk ``BASE_DIR/<engine>/<db>/`` and return every backup, oldest first per database."""
    base = Path(base_dir)
    entries: list[CatalogEntry] = []
    for engine in engines or [e.value for e in DbType]:
        engine_dir = base / engine
        if not engine_dir.is_dir():
            continue
        for db_dir in sorted(d for d in engine_dir.iterdir() if d.is_dir() and not d.is_symlink()):
            if database and db_dir.name != database:
                continue
            for f in _artifacts(db_dir):
                st = f.stat()
                name = f.name.lower()
                entries.append(CatalogEntry(
                    engine=engine,
                    database=db_dir.name,
                    file=str(f),
                    size=st.st_size,
                    mtime=st.st_mtime,
                    encrypted=name.endswith((".age", ".gpg")),
                    compressed=".gz" in name or name.endswith(".zip"),
                    incremental="_inc." in name,
                ))
    return entries


def size_anomaly(new_file: Path, ratio: float) -> str | None:
    """Return a warning when *new_file* is much smaller than the previous full backup of that database."""
    if ratio <= 0 or not new_file.exists():
        return None
    new_size = new_file.stat().st_size
    previous = [
        f for f in _artifacts(new_file.parent)
        if f != new_file and "_inc." not in f.name.lower() and f.suffixes[-2:] == new_file.suffixes[-2:]
    ]
    if "_inc." in new_file.name.lower() or not previous:
        return None
    prev = previous[-1]
    prev_size = prev.stat().st_size
    if prev_size > 0 and new_size < prev_size * ratio:
        return (
            f"{new_file.name} is {human_size(new_size)}, under {ratio:.0%} of the previous backup "
            f"{prev.name} ({human_size(prev_size)}) — possible silent failure"
        )
    return None
