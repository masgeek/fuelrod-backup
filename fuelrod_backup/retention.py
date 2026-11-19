"""GFS-style retention profiles, e.g. ``daily=7,weekly=4,monthly=12``."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

_KEYS = ("daily", "weekly", "monthly", "yearly")


def parse_profile(text: str) -> dict[str, int]:
    """Parse ``daily=7,weekly=4`` into a dict; raises ValueError on bad input."""
    profile: dict[str, int] = {}
    for part in filter(None, (p.strip() for p in text.split(","))):
        key, sep, val = part.partition("=")
        key = key.strip().lower()
        if not sep or key not in _KEYS or not val.strip().isdigit():
            raise ValueError(f"invalid retention entry {part!r} (use {', '.join(k + '=N' for k in _KEYS)})")
        profile[key] = int(val)
    return profile


def _bucket(ts: float, kind: str) -> tuple:
    d = datetime.fromtimestamp(ts)
    if kind == "daily":
        return (d.year, d.month, d.day)
    if kind == "weekly":
        iso = d.isocalendar()
        return (iso.year, iso.week)
    if kind == "monthly":
        return (d.year, d.month)
    return (d.year,)


def select_keep(files: list[Path], profile: dict[str, int]) -> set[Path]:
    """Newest file of each of the last N daily/weekly/monthly/yearly buckets is kept."""
    ordered = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    keep: set[Path] = set()
    for kind, count in profile.items():
        seen: list[tuple] = []
        for f in ordered:
            bucket = _bucket(f.stat().st_mtime, kind)
            if bucket in seen:
                continue
            if len(seen) >= count:
                break
            seen.append(bucket)
            keep.add(f)
    return keep
