"""Tests for pure helper functions in n8n_backup.py and n8n_restore.py."""

from pathlib import Path
from unittest.mock import MagicMock

from fuelrod_backup.n8n_backup import _is_container_running
from fuelrod_backup.n8n_restore import (
    _extract_timestamp_from_backup,
    _is_date_dir,
    _read_summary_field,
)

# ──────────────────────────────────────────────────────────────────────────────
#  _is_date_dir
# ──────────────────────────────────────────────────────────────────────────────

class TestIsDateDir:
    def test_valid_date(self):
        assert _is_date_dir("2024-01-15") is True

    def test_valid_date_end_of_year(self):
        assert _is_date_dir("2025-12-31") is True

    def test_wrong_separator(self):
        assert _is_date_dir("2024/01/15") is False

    def test_american_format(self):
        assert _is_date_dir("01-15-2024") is False

    def test_compact_format(self):
        assert _is_date_dir("20240115") is False

    def test_partial_date(self):
        assert _is_date_dir("2024-01") is False

    def test_empty_string(self):
        assert _is_date_dir("") is False

    def test_non_date_string(self):
        assert _is_date_dir("not-a-date") is False

    def test_with_trailing_text(self):
        # Must be a full match — no extra characters allowed.
        assert _is_date_dir("2024-01-15-extra") is False


# ──────────────────────────────────────────────────────────────────────────────
#  _extract_timestamp_from_backup
# ──────────────────────────────────────────────────────────────────────────────

class TestExtractTimestamp:
    def test_standard_filename(self):
        p = Path("n8n_hot_backup_20240101_120000_123.tar.gz")
        assert _extract_timestamp_from_backup(p) == "20240101_120000_123"

    def test_custom_service_name(self):
        p = Path("myservice_hot_backup_20250615_093000_456.tar.gz")
        assert _extract_timestamp_from_backup(p) == "20250615_093000_456"

    def test_no_marker_returns_none(self):
        p = Path("some_random_archive.tar.gz")
        assert _extract_timestamp_from_backup(p) is None

    def test_empty_timestamp_after_marker(self):
        # "_hot_backup_" present but nothing after it — returns empty string, not None.
        p = Path("n8n_hot_backup_.tar.gz")
        result = _extract_timestamp_from_backup(p)
        assert result == ""

    def test_no_tar_gz_extension(self):
        # Works even without the standard extension.
        p = Path("n8n_hot_backup_20240101_120000_123.bak")
        # The function strips only ".tar.gz", so "123.bak" remains in stem.
        result = _extract_timestamp_from_backup(p)
        assert result is not None


# ──────────────────────────────────────────────────────────────────────────────
#  _read_summary_field
# ──────────────────────────────────────────────────────────────────────────────

class TestReadSummaryField:
    SUMMARY_CONTENT = (
        "Service: n8n\n"
        "Volume Size: 12M\n"
        "Workflow Count: 42\n"
        "Database Files: 3\n"
    )

    def test_existing_field(self, tmp_path):
        f = tmp_path / "summary.txt"
        f.write_text(self.SUMMARY_CONTENT, encoding="utf-8")
        assert _read_summary_field(f, "Service") == "n8n"

    def test_field_with_spaces_in_value(self, tmp_path):
        f = tmp_path / "summary.txt"
        f.write_text(self.SUMMARY_CONTENT, encoding="utf-8")
        assert _read_summary_field(f, "Volume Size") == "12M"

    def test_numeric_value(self, tmp_path):
        f = tmp_path / "summary.txt"
        f.write_text(self.SUMMARY_CONTENT, encoding="utf-8")
        assert _read_summary_field(f, "Workflow Count") == "42"

    def test_missing_field_returns_none(self, tmp_path):
        f = tmp_path / "summary.txt"
        f.write_text(self.SUMMARY_CONTENT, encoding="utf-8")
        assert _read_summary_field(f, "Nonexistent") is None

    def test_file_not_found_returns_none(self, tmp_path):
        missing = tmp_path / "no_such_file.txt"
        assert _read_summary_field(missing, "Service") is None

    def test_returns_first_match(self, tmp_path):
        f = tmp_path / "summary.txt"
        f.write_text("Key: first\nKey: second\n", encoding="utf-8")
        assert _read_summary_field(f, "Key") == "first"

    def test_empty_file_returns_none(self, tmp_path):
        f = tmp_path / "summary.txt"
        f.write_text("", encoding="utf-8")
        assert _read_summary_field(f, "Service") is None


# ──────────────────────────────────────────────────────────────────────────────
#  _is_container_running
# ──────────────────────────────────────────────────────────────────────────────

class TestIsContainerRunning:
    def test_running_container_returns_true(self, mocker):
        mock_run = mocker.patch("fuelrod_backup.n8n_backup.subprocess.run")
        mock_run.return_value = MagicMock(stdout="n8n\n", returncode=0)
        assert _is_container_running("n8n") is True

    def test_stopped_container_returns_false(self, mocker):
        mock_run = mocker.patch("fuelrod_backup.n8n_backup.subprocess.run")
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        assert _is_container_running("n8n") is False

    def test_partial_name_match_returns_false(self, mocker):
        # "n8n" must not match "n8n-worker" — exact line comparison.
        mock_run = mocker.patch("fuelrod_backup.n8n_backup.subprocess.run")
        mock_run.return_value = MagicMock(stdout="n8n-worker\n", returncode=0)
        assert _is_container_running("n8n") is False

    def test_multiple_containers_exact_match(self, mocker):
        # Both "n8n" and "n8n-worker" listed — only "n8n" should match.
        mock_run = mocker.patch("fuelrod_backup.n8n_backup.subprocess.run")
        mock_run.return_value = MagicMock(stdout="n8n\nn8n-worker\n", returncode=0)
        assert _is_container_running("n8n") is True

    def test_whitespace_trimmed_from_lines(self, mocker):
        mock_run = mocker.patch("fuelrod_backup.n8n_backup.subprocess.run")
        mock_run.return_value = MagicMock(stdout="  n8n  \n", returncode=0)
        assert _is_container_running("n8n") is True
