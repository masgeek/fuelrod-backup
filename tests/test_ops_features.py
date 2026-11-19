"""Tests for locking, retention, catalog, hooks and schedule helpers."""

from __future__ import annotations

import os
import time

import pytest

from fuelrod_backup.catalog import list_backups, size_anomaly
from fuelrod_backup.hooks import HookError, run_hook
from fuelrod_backup.lock import LockError, run_lock
from fuelrod_backup.retention import parse_profile, select_keep
from fuelrod_backup.schedule import render


def test_lock_blocks_second_holder_and_releases(tmp_path):
    with run_lock(tmp_path, "postgres", "backup"):
        with pytest.raises(LockError):
            with run_lock(tmp_path, "postgres", "restore"):
                pass
    with run_lock(tmp_path, "postgres", "restore"):
        pass


def test_lock_stale_pid_is_reclaimed(tmp_path):
    import json
    import socket

    lock = tmp_path / ".fuelrod-postgres.lock"
    lock.write_text(json.dumps({"pid": 99999999, "host": socket.gethostname(), "operation": "backup"}))
    with run_lock(tmp_path, "postgres", "backup"):
        pass


def test_parse_profile():
    assert parse_profile("daily=7, weekly=4") == {"daily": 7, "weekly": 4}
    with pytest.raises(ValueError):
        parse_profile("hourly=3")


def test_select_keep_newest_per_bucket(tmp_path):
    now = time.time()
    files = []
    for i, age_days in enumerate([0, 0.1, 1, 2, 40]):
        f = tmp_path / f"f{i}.dump"
        f.write_text("x")
        os.utime(f, (now - age_days * 86400, now - age_days * 86400))
        files.append(f)
    keep = select_keep(files, {"daily": 2})
    assert files[0] in keep and len(keep) == 2


def test_catalog_and_size_anomaly(tmp_path):
    db = tmp_path / "postgres" / "app"
    db.mkdir(parents=True)
    old, new = db / "app_1.dump", db / "app_2.dump"
    old.write_bytes(b"x" * 1000)
    os.utime(old, (1, 1))
    new.write_bytes(b"x" * 10)
    entries = list_backups(tmp_path)
    assert [e.database for e in entries] == ["app", "app"]
    assert size_anomaly(new, 0.5) is not None
    assert size_anomaly(new, 0) is None


def test_hook_failure_raises():
    import sys

    run_hook("", "pre_backup", {})
    with pytest.raises(HookError):
        run_hook(f'"{sys.executable}" -c "import sys; sys.exit(3)"', "pre_backup", {})


def test_render_rejects_bad_cron():
    with pytest.raises(ValueError):
        render("not a cron", ["x"], "log")
