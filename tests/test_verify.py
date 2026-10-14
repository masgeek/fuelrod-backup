"""Tests for backup artifact verification."""

from __future__ import annotations

import gzip
import os
import zipfile
from pathlib import Path

from fuelrod_backup.config import Config, DbType
from fuelrod_backup.verify import (
    check_bak,
    check_sql,
    check_zip,
    classify_artifact,
    discover_artifacts,
    run_verify,
    sha256_file,
    verify_artifact,
)


def _touch(path: Path, data: bytes = b"x", mtime: int | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


# ──────────────────────────────────────────────────────────────────────────────
#  classify_artifact
# ──────────────────────────────────────────────────────────────────────────────

class TestClassifyArtifact:
    def test_pg_custom(self):
        assert classify_artifact(Path("app.dump")) == "pg_custom"

    def test_pg_custom_gz(self):
        assert classify_artifact(Path("app.dump.gz")) == "pg_custom"

    def test_sql_and_gz(self):
        assert classify_artifact(Path("app.sql")) == "sql"
        assert classify_artifact(Path("app.sql.gz")) == "sql"

    def test_zip_and_bak(self):
        assert classify_artifact(Path("app.zip")) == "zip"
        assert classify_artifact(Path("app.bak")) == "bak"

    def test_encrypted(self):
        assert classify_artifact(Path("app.dump.age")) == "encrypted:age"
        assert classify_artifact(Path("app.sql.gpg")) == "encrypted:gpg"

    def test_unknown(self):
        assert classify_artifact(Path("notes.txt")) == "unknown"


# ──────────────────────────────────────────────────────────────────────────────
#  Per-type checks
# ──────────────────────────────────────────��───────────────────────────────────

class TestCheckSql:
    def test_plain_sql_ok(self, tmp_path):
        f = _touch(tmp_path / "a.sql", b"-- dump\nCREATE TABLE t (id int);\n")
        checks = check_sql(f)
        assert all(c.status == "ok" for c in checks)

    def test_gzip_header_only_by_default(self, tmp_path):
        f = tmp_path / "a.sql.gz"
        f.write_bytes(gzip.compress(b"-- dump\nSELECT 1;\n"))
        statuses = {c.name: c.status for c in check_sql(f)}
        assert statuses["gzip"] == "skip"
        assert statuses["content"] == "ok"

    def test_deep_validates_the_gzip_crc(self, tmp_path):
        f = tmp_path / "a.sql.gz"
        f.write_bytes(gzip.compress(b"-- dump\nSELECT 1;\n"))
        assert any(c.status == "ok" and "uncompressed" in c.detail for c in check_sql(f, deep=True))

    def test_truncated_gzip_fails_in_deep_mode(self, tmp_path):
        f = tmp_path / "a.sql.gz"
        f.write_bytes(gzip.compress(b"SELECT 1;\n")[:-5])  # chop the CRC/ISIZE tail
        assert any(c.status == "fail" for c in check_sql(f, deep=True))

    def test_truncated_gzip_fails_even_from_the_header(self, tmp_path):
        # A payload cut mid-stream breaks the header probe too, so the default
        # (non-deep) pass still catches gross truncation.
        f = tmp_path / "a.sql.gz"
        raw = gzip.compress(b"SELECT 1;\n" * 100)
        f.write_bytes(raw[: len(raw) // 2])
        assert any(c.status == "fail" for c in check_sql(f))

    def test_garbage_payload_fails(self, tmp_path):
        f = _touch(tmp_path / "a.sql.gz", b"\x1f\x8b\x08\x00" + b"garbage" * 40)
        assert any(c.status == "fail" for c in check_sql(f))

    def test_empty_file_fails(self, tmp_path):
        f = _touch(tmp_path / "a.sql", b"")
        assert any(c.status == "fail" for c in check_sql(f))

    def test_non_sql_header_warns(self, tmp_path):
        f = _touch(tmp_path / "a.sql", b"\x89PNG\r\n\x1a\n binary junk")
        assert any(c.status == "warn" for c in check_sql(f))


class TestCheckZip:
    def test_valid_archive_skips_crc_by_default(self, tmp_path):
        f = tmp_path / "a.zip"
        with zipfile.ZipFile(f, "w") as zf:
            zf.writestr("dump.sql", "SELECT 1;\n")
        statuses = {c.name: c.status for c in check_zip(f)}
        assert statuses["readable"] == "ok"
        assert statuses["crc"] == "skip"

    def test_deep_verifies_every_entry(self, tmp_path):
        f = tmp_path / "a.zip"
        with zipfile.ZipFile(f, "w") as zf:
            zf.writestr("dump.sql", "SELECT 1;\n")
        assert all(c.status == "ok" for c in check_zip(f, deep=True))

    def test_deep_detects_corrupt_entry(self, tmp_path):
        f = tmp_path / "a.zip"
        with zipfile.ZipFile(f, "w") as zf:
            zf.writestr("dump.sql", "SELECT 1;\n" * 500)
        raw = bytearray(f.read_bytes())
        raw[len(raw) // 2] ^= 0xFF  # flip a byte inside the compressed payload
        f.write_bytes(bytes(raw))
        assert any(c.status == "fail" for c in check_zip(f, deep=True))

    def test_empty_archive_fails(self, tmp_path):
        f = tmp_path / "a.zip"
        with zipfile.ZipFile(f, "w"):
            pass
        assert any(c.status == "fail" for c in check_zip(f))

    def test_corrupt_archive_fails(self, tmp_path):
        f = _touch(tmp_path / "a.zip", b"PK\x03\x04 not really a zip")
        assert any(c.status == "fail" for c in check_zip(f))


class TestCheckBak:
    def test_valid_header_ok(self, tmp_path):
        f = _touch(tmp_path / "a.bak", b"MICROSOFT\x00\x01payload")
        assert all(c.status == "ok" for c in check_bak(f))

    def test_bad_header_warns(self, tmp_path):
        f = _touch(tmp_path / "a.bak", b"NOPE" * 8)
        assert any(c.status == "warn" for c in check_bak(f))

    def test_empty_fails(self, tmp_path):
        f = _touch(tmp_path / "a.bak", b"")
        assert any(c.status == "fail" for c in check_bak(f))


class TestSha256:
    def test_matches_hashlib(self, tmp_path):
        import hashlib
        data = b"payload" * 1000
        f = _touch(tmp_path / "a.bin", data)
        assert sha256_file(f) == hashlib.sha256(data).hexdigest()


# ──────────────────────────────────────────────────────────────────────────────
#  verify_artifact status aggregation
# ──────────────────────────────────────────────────────────────────────────────

class TestVerifyArtifact:
    def test_good_sql_is_ok(self, tmp_path):
        f = _touch(tmp_path / "a.sql", b"-- dump\nCREATE TABLE t (id int);\n")
        result = verify_artifact(f, None)
        assert result.ok
        assert result.status == "ok"

    def test_corrupt_artifact_fails(self, tmp_path):
        f = _touch(tmp_path / "a.zip", b"PK\x03\x04 broken")
        result = verify_artifact(f, None)
        assert not result.ok
        assert result.status == "fail"

    def test_unknown_type_warns(self, tmp_path):
        f = _touch(tmp_path / "a.txt", b"hello")
        assert verify_artifact(f, None).status == "warn"

    def test_checksum_appends_a_pass(self, tmp_path):
        f = _touch(tmp_path / "a.sql", b"-- dump\nSELECT 1;\n")
        result = verify_artifact(f, None, checksum=True)
        assert "sha256" in {c.name for c in result.checks}


# ──────────────────────────────────────────────────────────────────────────────
#  Discovery
# ──────────────────────────────────────────────────────────────────────────────

class TestDiscoverArtifacts:
    def _tree(self, root: Path) -> None:
        _touch(root / "proj1" / "postgres" / "app" / "a.dump", mtime=100)
        _touch(root / "proj1" / "postgres" / "app" / "b.dump", mtime=300)
        _touch(root / "proj1" / "mariadb" / "app" / "c.sql.gz", mtime=200)
        _touch(root / "proj2" / "postgres" / "other" / "d.dump", mtime=100)

    def test_finds_all(self, tmp_path):
        self._tree(tmp_path)
        assert len(discover_artifacts(tmp_path)) == 4

    def test_filter_by_project(self, tmp_path):
        self._tree(tmp_path)
        names = {p.name for p in discover_artifacts(tmp_path, project="proj1")}
        assert names == {"a.dump", "b.dump", "c.sql.gz"}

    def test_filter_by_engine_and_db(self, tmp_path):
        self._tree(tmp_path)
        names = {p.name for p in discover_artifacts(tmp_path, engine="mariadb", database="app")}
        assert names == {"c.sql.gz"}

    def test_latest_only_keeps_newest_per_database(self, tmp_path):
        self._tree(tmp_path)
        found = discover_artifacts(tmp_path, latest_only=True)
        assert [p.name for p in found] == ["c.sql.gz", "b.dump", "d.dump"]

    def test_missing_base_dir_aborts(self, tmp_path):
        import pytest
        with pytest.raises(SystemExit):
            discover_artifacts(tmp_path / "nope")

    def test_flat_layout_is_still_found(self, tmp_path):
        _touch(tmp_path / "loose" / "backup.sql")
        assert [p.name for p in discover_artifacts(tmp_path)] == ["backup.sql"]


# ──────────────────────────────────────────────────────────────────────────────
#  run_verify exit code
# ──────────────────────────────────────────────────────────────────────────────

def _cfg(base: Path) -> Config:
    c = Config()
    c.db_type = DbType.MARIADB
    c.base_dir = str(base)
    return c


class TestRunVerify:
    def test_zero_when_everything_passes(self, tmp_path):
        _touch(tmp_path / "p" / "mariadb" / "app" / "a.sql", b"-- dump\nSELECT 1;\n")
        assert run_verify(_cfg(tmp_path)) == 0

    def test_one_when_something_fails(self, tmp_path):
        _touch(tmp_path / "p" / "mariadb" / "app" / "a.sql", b"-- dump\nSELECT 1;\n")
        _touch(tmp_path / "p" / "mariadb" / "app" / "b.zip", b"PK\x03\x04 broken")
        assert run_verify(_cfg(tmp_path)) == 1

    def test_empty_base_is_not_a_failure(self, tmp_path):
        tmp_path.mkdir(exist_ok=True)
        assert run_verify(_cfg(tmp_path)) == 0

    def test_missing_base_dir_setting_aborts(self, tmp_path):
        import pytest
        empty = Config()
        empty.db_type = DbType.MARIADB
        empty.base_dir = ""
        with pytest.raises(SystemExit):
            run_verify(empty)
