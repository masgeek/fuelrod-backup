"""Backward-compatible Google Drive sync entry point.

The implementation now lives in `storage.py`, which handles any rclone remote
(S3, B2, SFTP, …) as well as Google Drive. This module keeps the original
`run_gdrive_sync()` signature and the `gdrive:<GDRIVE>` default so existing
callers and scripts are unaffected.
"""

from __future__ import annotations

from .config import Config
from .storage import run_sync


def run_gdrive_sync(
        cfg: Config,
        *,
        dry_run: bool = False,
        gdrive_remote: str | None = None,
        age_days: int | None = None,
        include_patterns: list[str] | None = None,
        delete_local: bool = True,
) -> None:
    """Sync local backups to Google Drive via rclone, then prune old remote files."""
    remote = f"gdrive:{gdrive_remote}" if gdrive_remote else f"gdrive:{cfg.gdrive_remote}"
    run_sync(
        cfg,
        remote=remote,
        dry_run=dry_run,
        age_days=age_days,
        include_patterns=include_patterns,
        delete_local=delete_local,
    )
