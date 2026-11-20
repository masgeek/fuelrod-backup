"""Small file helpers shared by backup/restore code."""

from __future__ import annotations

import gzip
import shutil
import tempfile
from pathlib import Path


def gunzip_to_temp(src: Path, suffix: str = ".dump") -> Path:
    """Stream-decompress *src* into a temp file and return its path (caller deletes it)."""
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp_file:
        tmp = Path(tmp_file.name)
    try:
        with gzip.open(src, "rb") as f_in, tmp.open("wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return tmp


def human_size(num_bytes: int | float) -> str:
    """Format a byte count as B / KB / MB / GB / TB (1024-based), e.g. ``1.5 GB``."""
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"
