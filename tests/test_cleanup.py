"""Tests for retention-cleanup logic in backup.py and n8n_backup.py."""

import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fuelrod_backup.backup import _cleanup_old as backup_cleanup
from fuelrod_backup.n8n_backup import _cleanup_old as n8n_cleanup

# ──────────────────────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_file(path: Path, age_days: float = 0) -> Path:
    """Create *path* and set its mtime to *age_days* days in the past."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    if age_days > 0:
        old_ts = time.time() - age_days * 86400
        os.utime(path, (old_ts, old_ts))
    return path


# ──────────────────────────────────────────────────────────────────────────────
#  backup._cleanup_old
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def silence_backup_console(monkeypatch):
    monkeypatch.setattr("fuelrod_backup.backup.console", MagicMock())


@pytest.mark.usefixtures("silence_backup_console")
class TestBackupCleanup:
    def test_zero_days_skips_cleanup(self, tmp_path):
        old_file = _make_file(tmp_path / "db" / "dump_old.dump", age_days=30)
        backup_cleanup(str(tmp_path), days=0)
        assert old_file.exists()

    def test_negative_days_skips_cleanup(self, tmp_path):
        old_file = _make_file(tmp_path / "db" / "dump_old.dump", age_days=30)
        backup_cleanup(str(tmp_path), days=-1)
        assert old_file.exists()

    def test_old_dump_deleted(self, tmp_path):
        old_file = _make_file(tmp_path / "db" / "dump_old.dump", age_days=30)
        backup_cleanup(str(tmp_path), days=7)
        assert not old_file.exists()

    def test_recent_dump_kept(self, tmp_path):
        new_file = _make_file(tmp_path / "db" / "dump_new.dump", age_days=1)
        backup_cleanup(str(tmp_path), days=7)
        assert new_file.exists()

    def test_all_backup_extensions_cleaned(self, tmp_path):
        db_dir = tmp_path / "db"
        files = [
            _make_file(db_dir / "a.dump", age_days=10),
            _make_file(db_dir / "b.dump.gz", age_days=10),
            _make_file(db_dir / "c.sql", age_days=10),
            _make_file(db_dir / "d.sql.gz", age_days=10),
            _make_file(db_dir / "e.bak", age_days=10),
            _make_file(db_dir / "manifest_20240101.txt", age_days=10),
        ]
        backup_cleanup(str(tmp_path), days=7)
        for f in files:
            assert not f.exists(), f"{f.name} should have been deleted"

    def test_empty_subdir_removed_after_cleanup(self, tmp_path):
        db_dir = tmp_path / "mydb"
        _make_file(db_dir / "old.dump", age_days=10)
        backup_cleanup(str(tmp_path), days=7)
        assert not db_dir.exists()

    def test_non_empty_subdir_kept(self, tmp_path):
        db_dir = tmp_path / "mydb"
        _make_file(db_dir / "old.dump", age_days=10)
        _make_file(db_dir / "new.dump", age_days=1)  # kept — dir should not be removed
        backup_cleanup(str(tmp_path), days=7)
        assert db_dir.exists()

    def test_mixed_old_and_new_files(self, tmp_path):
        db_dir = tmp_path / "db"
        old = _make_file(db_dir / "old.dump", age_days=30)
        new = _make_file(db_dir / "new.dump", age_days=1)
        backup_cleanup(str(tmp_path), days=7)
        assert not old.exists()
        assert new.exists()


# ──────────────────────────────────────────────────────────────────────────────
#  n8n_backup._cleanup_old
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def silence_n8n_console(monkeypatch):
    monkeypatch.setattr("fuelrod_backup.n8n_backup.console", MagicMock())


@pytest.mark.usefixtures("silence_n8n_console")
class TestN8nCleanup:
    # n8n cleanup compares directory NAMES (YYYY-MM-DD) against datetime.now(),
    # so we freeze "now" to a known date and create dirs relative to it.

    NOW = datetime(2024, 6, 15)

    def _dated_dir(self, base: Path, delta_days: int) -> Path:
        date = self.NOW - timedelta(days=delta_days)
        d = base / date.strftime("%Y-%m-%d")
        d.mkdir(parents=True, exist_ok=True)
        return d

    @pytest.fixture()
    def frozen_now(self):
        with patch("fuelrod_backup.n8n_backup.datetime") as mock_dt:
            mock_dt.now.return_value = self.NOW
            mock_dt.strptime = datetime.strptime  # keep real strptime
            yield mock_dt

    def test_zero_days_skips_cleanup(self, tmp_path, frozen_now):
        old_dir = self._dated_dir(tmp_path, delta_days=30)
        n8n_cleanup(tmp_path, days_to_keep=0)
        assert old_dir.exists()

    def test_old_dated_dir_removed(self, tmp_path, frozen_now):
        old_dir = self._dated_dir(tmp_path, delta_days=30)
        n8n_cleanup(tmp_path, days_to_keep=7)
        assert not old_dir.exists()

    def test_recent_dated_dir_kept(self, tmp_path, frozen_now):
        new_dir = self._dated_dir(tmp_path, delta_days=2)
        n8n_cleanup(tmp_path, days_to_keep=7)
        assert new_dir.exists()

    def test_non_date_dirs_ignored(self, tmp_path, frozen_now):
        other = tmp_path / "not-a-date"
        other.mkdir()
        n8n_cleanup(tmp_path, days_to_keep=7)
        assert other.exists()

    def test_exactly_at_cutoff_kept(self, tmp_path, frozen_now):
        # A dir dated exactly `days_to_keep` days ago should NOT be removed
        # because the comparison is strict (<, not <=).
        cutoff_dir = self._dated_dir(tmp_path, delta_days=7)
        n8n_cleanup(tmp_path, days_to_keep=7)
        assert cutoff_dir.exists()

    def test_mixed_dirs(self, tmp_path, frozen_now):
        old = self._dated_dir(tmp_path, delta_days=30)
        new = self._dated_dir(tmp_path, delta_days=2)
        n8n_cleanup(tmp_path, days_to_keep=7)
        assert not old.exists()
        assert new.exists()
