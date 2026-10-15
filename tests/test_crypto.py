"""Tests for encryption at rest (crypto.py) and its backup integration."""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from fuelrod_backup import crypto
from fuelrod_backup.backup import _backup_one, _cleanup_old
from fuelrod_backup.config import Config, DbType
from fuelrod_backup.crypto import CryptoError

TOOL = "gpg" if shutil.which("gpg") else None
needs_tool = pytest.mark.skipif(TOOL is None, reason="neither age nor gpg installed")


# ──────────────────────────────────────────────────────────────────────────────
#  Config validation
# ──────────────────────────────────────────────────────────────────────────────

def _cfg(**kw) -> Config:
    c = Config()
    c.encrypt_tool = "age"
    for k, v in kw.items():
        setattr(c, k, v)
    return c


class TestValidateConfig:
    def test_disabled_is_a_noop(self):
        crypto.validate_config(_cfg(encrypt=False))  # must not raise

    def test_missing_tool_fails(self, monkeypatch):
        monkeypatch.setattr(crypto.shutil, "which", lambda _: None)
        with pytest.raises(CryptoError, match="not found in PATH"):
            crypto.validate_config(_cfg(encrypt=True, encrypt_recipient="age1abc"))

    def test_age_requires_recipient(self, monkeypatch):
        monkeypatch.setattr(crypto.shutil, "which", lambda _: "/usr/bin/age")
        with pytest.raises(CryptoError, match="ENCRYPT_RECIPIENT"):
            crypto.validate_config(_cfg(encrypt=True))

    def test_gpg_needs_recipient_or_passphrase(self, monkeypatch):
        monkeypatch.setattr(crypto.shutil, "which", lambda _: "/usr/bin/gpg")
        with pytest.raises(CryptoError, match="ENCRYPT_RECIPIENT"):
            crypto.validate_config(_cfg(encrypt=True, encrypt_tool="gpg"))

    def test_missing_passphrase_file_fails(self, monkeypatch):
        monkeypatch.setattr(crypto.shutil, "which", lambda _: "/usr/bin/gpg")
        with pytest.raises(CryptoError, match="Passphrase file"):
            crypto.validate_config(_cfg(
                encrypt=True, encrypt_tool="gpg", encrypt_passphrase_file="/nope/pass.txt"
            ))

    def test_valid_age_config_passes(self, monkeypatch):
        monkeypatch.setattr(crypto.shutil, "which", lambda _: "/usr/bin/age")
        crypto.validate_config(_cfg(encrypt=True, encrypt_recipient="age1abc"))


class TestIsEncrypted:
    @pytest.mark.parametrize("name,expected", [
        ("a.dump.age", True),
        ("a.sql.gpg", True),
        ("a.dump.gz", False),
        ("a.dump", False),
    ])
    def test_suffix_detection(self, name, expected):
        assert crypto.is_encrypted(Path(name)) is expected


# ──────────────────────────────────────────────────────────────────────────────
#  Round trip (needs a real crypto tool)
# ──────────────────────────────────────────────────────────────────────────────

@needs_tool
class TestGpgRoundTrip:
    def _cfg(self, passphrase_file: Path) -> Config:
        c = Config()
        c.encrypt = True
        c.encrypt_tool = "gpg"
        c.encrypt_passphrase_file = str(passphrase_file)
        return c

    def test_encrypt_then_decrypt(self, tmp_path):
        pf = tmp_path / "pass.txt"
        pf.write_text("hunter2\n", encoding="utf-8")
        cfg = self._cfg(pf)
        payload = b"secret dump payload" * 500
        src = tmp_path / "app.dump.gz"
        src.write_bytes(payload)

        enc = crypto.encrypt_file(src, cfg)

        assert enc.name == "app.dump.gz.gpg"
        assert not src.exists(), "plaintext must be removed"
        assert enc.read_bytes() != payload

        dec = crypto.decrypt_file(enc, cfg)
        assert dec.read_bytes() == payload

    def test_wrong_passphrase_reports_clearly(self, tmp_path):
        pf = tmp_path / "pass.txt"
        pf.write_text("right\n", encoding="utf-8")
        cfg = self._cfg(pf)
        src = tmp_path / "app.sql"
        src.write_bytes(b"payload")
        enc = crypto.encrypt_file(src, cfg)

        pf.write_text("wrong\n", encoding="utf-8")
        with pytest.raises(CryptoError, match="Decryption failed"):
            crypto.decrypt_file(enc, cfg)

    def test_failed_encryption_keeps_the_plaintext(self, tmp_path):
        pf = tmp_path / "pass.txt"
        pf.write_text("hunter2\n", encoding="utf-8")
        cfg = self._cfg(pf)
        cfg.encrypt_recipient = "definitely-not-a-key@invalid.invalid"
        src = tmp_path / "app.sql"
        src.write_bytes(b"payload")

        with pytest.raises(CryptoError):
            crypto.encrypt_file(src, cfg)

        assert src.exists(), "a failed encryption must never destroy the only copy"


class TestDecryptWithMockedTool:
    def _fake_run(self, payload: bytes = b"plain bytes"):
        def run(cmd, **kwargs):
            # cmd is [..., '-o', dest, src] or [..., '--output', dest, ..., src]
            dest = cmd[cmd.index("-o") + 1] if "-o" in cmd else cmd[cmd.index("--output") + 1]
            Path(dest).write_bytes(payload)
            return MagicMock(returncode=0)

        return run

    def test_decrypt_writes_temp_file(self, tmp_path, monkeypatch):
        src = tmp_path / "app.dump.age"
        src.write_bytes(b"header")
        monkeypatch.setattr(crypto.shutil, "which", lambda _: "/usr/bin/age")
        monkeypatch.setattr(crypto.subprocess, "run", self._fake_run())

        cfg = Config()
        cfg.encrypt_identity_file = str(tmp_path / "id.txt")
        out = crypto.decrypt_file(src, cfg)

        assert out.read_bytes() == b"plain bytes"
        assert out != src
        assert out in crypto._TEMP_FILES, "temp plaintext must be registered for cleanup"

    def test_empty_output_is_rejected(self, tmp_path, monkeypatch):
        src = tmp_path / "app.dump.age"
        src.write_bytes(b"header")
        monkeypatch.setattr(crypto.shutil, "which", lambda _: "/usr/bin/age")
        monkeypatch.setattr(crypto.subprocess, "run", self._fake_run(b""))

        with pytest.raises(CryptoError, match="empty file"):
            crypto.decrypt_file(src, Config())

    def test_missing_tool_is_reported(self, tmp_path, monkeypatch):
        src = tmp_path / "app.dump.age"
        src.write_bytes(b"header")
        monkeypatch.setattr(crypto.shutil, "which", lambda _: None)
        with pytest.raises(CryptoError, match="not found in PATH"):
            crypto.decrypt_file(src, Config())

    def test_decrypt_if_needed_passes_plain_files_through(self, tmp_path):
        plain = tmp_path / "app.dump"
        assert crypto.decrypt_if_needed(plain, Config()) is plain


class TestToolEnv:
    def test_does_not_leak_database_credentials(self, monkeypatch):
        monkeypatch.setenv("DB_PASSWORD", "super-secret")
        monkeypatch.setenv("PGPASSWORD", "pg-secret")
        env = crypto._tool_env()
        assert "DB_PASSWORD" not in env
        assert "PGPASSWORD" not in env

    def test_keeps_the_home_directory(self, monkeypatch):
        monkeypatch.setenv("HOME", "/home/op")
        assert crypto._tool_env()["HOME"] == "/home/op"


# ──────────────────────────────────────────────────────────────────────────────
#  Backup integration
# ──────────────────────────────────────────────────────────────────────────────

class TestBackupEncryption:
    def _cfg(self, tmp_path) -> Config:
        c = Config()
        c.db_type = DbType.POSTGRES
        c.base_dir = str(tmp_path)
        c.encrypt = True
        c.encrypt_tool = "gpg"
        return c

    def _adapter(self) -> MagicMock:
        a = MagicMock()
        a.dump_extension = ".dump"
        a.supports_incremental = False
        a.backup_db.side_effect = lambda db, out, **kw: out.write_bytes(b"dump bytes")
        return a

    def test_artifact_is_encrypted_and_plaintext_removed(self, tmp_path):
        pf = tmp_path / "pass.txt"
        pf.write_text("hunter2\n", encoding="utf-8")
        cfg = self._cfg(tmp_path)
        cfg.encrypt_passphrase_file = str(pf)

        out = _backup_one("app", cfg, self._adapter())

        assert out is not None and out.name.endswith(".dump.gpg")
        assert not (tmp_path / "postgres" / "app" / out.name.replace(".gpg", "")).exists()

    def test_manifest_records_encryption(self, tmp_path):
        pf = tmp_path / "pass.txt"
        pf.write_text("hunter2\n", encoding="utf-8")
        cfg = self._cfg(tmp_path)
        cfg.encrypt_passphrase_file = str(pf)

        _backup_one("app", cfg, self._adapter())
        manifest = sorted((tmp_path / "postgres" / "app").glob("manifest_*.txt"))[-1].read_text()

        assert "Encrypted  : gpg" in manifest

    def test_encryption_failure_aborts_without_losing_the_dump(self, tmp_path, monkeypatch):
        cfg = self._cfg(tmp_path)  # no passphrase file → validate_config fails
        with pytest.raises(SystemExit):
            _backup_one("app", cfg, self._adapter())

    def test_retention_covers_encrypted_artifacts(self, tmp_path):
        old = tmp_path / "postgres" / "app" / "app_old.dump.gz.age"
        old.parent.mkdir(parents=True)
        old.write_bytes(b"x")
        import os
        os.utime(old, (1, 1))

        _cleanup_old(str(tmp_path / "postgres"), days=1)
        assert not old.exists()

    def test_unaffected_when_disabled(self, tmp_path):
        cfg = self._cfg(tmp_path)
        cfg.encrypt = False
        out = _backup_one("app", cfg, self._adapter())
        assert out is not None and out.suffix == ".dump"
