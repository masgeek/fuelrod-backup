"""Tests for the rclone storage backends."""

from __future__ import annotations

from pathlib import Path

import pytest

from fuelrod_backup import storage
from fuelrod_backup.config import Config
from fuelrod_backup.storage import (
    StorageError,
    list_remotes,
    parse_remote,
    provider_args,
    rclone_env,
    run_sync,
)

# ──────────────────────────────────────────────────────────────────────────────
#  Remote parsing
# ──────────────────────────────────────────────────────────────────────────────

class TestParseRemote:
    @pytest.mark.parametrize("spec,provider,path", [
        ("s3:my-bucket/backups", "s3", "my-bucket/backups"),
        ("b2:bucket", "b2", "bucket"),
        ("sftp:host:/srv/backups", "sftp", "host:/srv/backups"),
    ])
    def test_provider_prefixes(self, spec, provider, path):
        remote = parse_remote(spec)
        assert (remote.provider, remote.path) == (provider, path)

    def test_trailing_slash_is_normalised(self):
        assert parse_remote("s3:backups/").path == "backups"

    def test_case_insensitive_provider(self):
        assert parse_remote("S3:bucket").provider == "s3"

    def test_bare_path_is_local(self):
        remote = parse_remote("/mnt/offsite/backups")
        assert remote.provider == "local"
        assert remote.path == "/mnt/offsite/backups"

    def test_empty_spec_rejected(self):
        with pytest.raises(ValueError):
            parse_remote("   ")

    def test_target_round_trips(self):
        assert parse_remote("s3:bucket/x").target == "s3:bucket/x"


class TestProviderArgs:
    def test_s3_provider_pinned_when_configured(self):
        cfg = Config()
        cfg.s3_provider = "Minio"
        assert "--s3-provider" in provider_args("s3", cfg)

    def test_s3_without_config_adds_nothing(self):
        assert provider_args("s3", Config()) == []

    def test_unknown_provider_adds_nothing(self):
        assert provider_args("sftp", Config()) == []


# ──────────────────────────────────────────────────────────────────────────────
#  Environment isolation
# ──────────────────────────────────────────────────────────────────────────────

class TestRcloneEnv:
    def test_excludes_credentials(self, monkeypatch):
        monkeypatch.setenv("DB_PASSWORD", "super-secret")
        monkeypatch.setenv("PGPASSWORD", "pg-secret")
        env = rclone_env()
        assert "DB_PASSWORD" not in env
        assert "PGPASSWORD" not in env

    def test_keeps_home_and_path(self, monkeypatch):
        monkeypatch.setenv("HOME", "/home/op")
        monkeypatch.setenv("PATH", "/usr/bin")
        env = rclone_env()
        assert env["HOME"] == "/home/op"
        assert env["PATH"] == "/usr/bin"


# ──────────────────────────────────────────────────────────────────────────────
#  rclone invocation
# ──────────────────────────────────────────────────────────────────────────────

class FakeCompleted:
    def __init__(self, returncode=0, stdout=b"", stderr=b""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class TestListRemotes:
    def test_list_remotes(self, monkeypatch):
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(
            storage.subprocess, "run",
            lambda *a, **kw: FakeCompleted(stdout=b"s3:\nb2:\n"),
        )
        assert list_remotes() == ["s3", "b2"]

    def test_failure_is_reported(self, monkeypatch):
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(
            storage.subprocess, "run",
            lambda *a, **kw: FakeCompleted(returncode=1, stderr=b"config unreadable"),
        )
        with pytest.raises(SystemExit):
            list_remotes()


# ──────────────────────────────────────────────────────────────────────────────
#  run_sync
# ──────────────────────────────────────────────────────────────────────────────

def _cfg(base: Path) -> Config:
    c = Config()
    c.base_dir = str(base)
    c.s3_remote = "db-backup"
    c.s3_bucket = "backups"
    return c


@pytest.fixture()
def tree(tmp_path):
    # Now we must create the engine sub-directory because run_sync uses cfg.backup_dir
    engine_dir = tmp_path / "postgres"
    engine_dir.mkdir()
    (engine_dir / "proj" / "postgres" / "app").mkdir(parents=True)
    (engine_dir / "proj" / "postgres" / "app" / "app_20260101.sql.gz").write_bytes(b"x" * 100)
    return tmp_path


class TestRunSync:
    def _patch(self, monkeypatch, calls, copy_rc=0):
        monkeypatch.setattr(storage, "rclone_available", lambda: True)

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[1] == "copy":
                return FakeCompleted(returncode=copy_rc, stderr=b"access denied" if copy_rc else b"")
            return FakeCompleted()

        monkeypatch.setattr(storage.subprocess, "run", fake_run)

    def test_dry_run_changes_nothing(self, monkeypatch, tree):
        calls: list = []
        self._patch(monkeypatch, calls)

        assert run_sync(_cfg(tree), remote="s3:bucket/x", dry_run=True, delete_local=False, interactive=False) == 0

        assert [c for c in calls if c[1] == "copy"] == [], "dry run must not copy"
        assert (tree / "postgres" / "proj" / "postgres" / "app" / "app_20260101.sql.gz").exists()

    def test_upload_then_local_delete(self, monkeypatch, tree):
        calls: list = []
        self._patch(monkeypatch, calls)

        assert run_sync(_cfg(tree), remote="s3:bucket/x", delete_local=True, interactive=False, include_patterns=["*.sql.gz"]) == 0

        assert any(c[1] == "copy" for c in calls)
        assert not (tree / "postgres" / "proj" / "postgres" / "app" / "app_20260101.sql.gz").exists()

    def test_keep_local_leaves_files(self, monkeypatch, tree):
        calls: list = []
        self._patch(monkeypatch, calls)

        run_sync(_cfg(tree), remote="s3:bucket/x", delete_local=False, interactive=False, include_patterns=["*.sql.gz"])

        assert (tree / "postgres" / "proj" / "postgres" / "app" / "app_20260101.sql.gz").exists()

    def test_failed_upload_keeps_local_files(self, monkeypatch, tree):
        calls: list = []
        self._patch(monkeypatch, calls, copy_rc=1)

        with pytest.raises(SystemExit) as exc:
            run_sync(_cfg(tree), remote="s3:bucket/x", delete_local=True, interactive=False, include_patterns=["*.sql.gz"])

        assert exc.value.code == 1
        assert (tree / "postgres" / "proj" / "postgres" / "app" / "app_20260101.sql.gz").exists(), \
            "a failed upload must never delete the local backup"

    def test_prune_failure_does_not_abort(self, monkeypatch, tree):
        def fake_run(cmd, **kwargs):
            if cmd[1] == "copy":
                return FakeCompleted()
            return FakeCompleted(returncode=1, stderr=b"no such bucket\n")

        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage.subprocess, "run", fake_run)

        assert run_sync(_cfg(tree), remote="s3:bucket/x", delete_local=False, interactive=False, include_patterns=["*.sql.gz"]) == 0

    def test_no_matching_files_is_a_success(self, monkeypatch, tmp_path):
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage.subprocess, "run", lambda *a, **kw: FakeCompleted())
        (tmp_path / "postgres").mkdir()
        cfg = _cfg(tmp_path)
        # We use a dummy include pattern to ensure no files match
        # We'll pass it to run_sync instead of setting it on cfg
        assert run_sync(cfg, remote="s3:bucket/x", include_patterns=["*.nothing"], interactive=False) == 0

    def test_missing_base_dir_aborts(self, monkeypatch, tmp_path):
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        cfg = _cfg(tmp_path)
        cfg.base_dir = str(tmp_path / "gone")
        with pytest.raises(SystemExit):
            run_sync(cfg, remote="s3:bucket/x")

    def test_empty_remote_aborts(self, monkeypatch, tree):
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        with pytest.raises(SystemExit):
            run_sync(_cfg(tree), remote="   ")

    def test_list_remotes_short_circuits(self, monkeypatch, tree):
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage, "list_remotes", lambda: ["s3"])
        assert run_sync(_cfg(tree), show_remotes=True) == 0


class TestUploadHelper:
    def test_raises_on_failure(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            storage.subprocess, "run",
            lambda *a, **kw: FakeCompleted(returncode=1, stderr=b"x\naccess denied"),
        )
        with pytest.raises(StorageError, match="access denied"):
            storage._upload(tmp_path, "s3:b/x", [], 1)

    def test_uses_list_form_without_shell(self, monkeypatch, tmp_path):
        calls = []
        monkeypatch.setattr(
            storage.subprocess, "run",
            lambda cmd, **kw: calls.append((cmd, kw)) or FakeCompleted(),
        )
        storage._upload(tmp_path, "s3:b/x", ["--include", "*.gz"], 1)

        cmd, kwargs = calls[0]
        assert cmd[:2] == ["rclone", "copy"]
        assert "shell" not in kwargs
        assert kwargs["check"] is False

    def test_missing_binary_is_reported(self, monkeypatch, tmp_path):
        def missing(*a, **kw):
            raise FileNotFoundError

        monkeypatch.setattr(storage.subprocess, "run", missing)
        with pytest.raises(StorageError, match="not found"):
            storage._upload(tmp_path, "s3:b/x", [], 1)
