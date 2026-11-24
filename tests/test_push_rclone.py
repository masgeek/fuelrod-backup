"""Tests for PULL_REMOTE / PUSH_REMOTE, push-init and the configurable rclone executable."""

from __future__ import annotations

import shutil
from types import SimpleNamespace

import pytest

from fuelrod_backup import pull, storage
from fuelrod_backup.config import Config, _load_pull
from fuelrod_backup.storage import run_sync


@pytest.fixture(autouse=True)
def _isolate():
    yield
    storage.use_rclone(None)


class TestRcloneBinary:
    def test_default_and_override(self):
        assert storage.rclone_bin() == "rclone"
        storage.use_rclone('  "D:/tools/rclone.exe" ')
        assert storage.rclone_bin() == "D:/tools/rclone.exe"
        storage.use_rclone("")
        assert storage.rclone_bin() == "rclone"

    def test_availability_follows_configured_binary(self, monkeypatch):
        monkeypatch.setattr(shutil, "which", lambda name: name if name == "D:/tools/rclone.exe" else None)
        assert not storage.rclone_available()
        storage.use_rclone("D:/tools/rclone.exe")
        assert storage.rclone_available()

    def test_pull_and_sync_invoke_the_configured_binary(self, monkeypatch):
        seen = []

        def fake(cmd, **kw):
            seen.append(cmd[0])
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

        monkeypatch.setattr(pull.subprocess, "run", fake)
        monkeypatch.setattr(storage.subprocess, "run", fake)
        storage.use_rclone("D:/tools/rclone.exe")
        pull._lsf_dirs("s3:b", [])
        storage._run_rclone(["version"])
        assert seen == ["D:/tools/rclone.exe", "D:/tools/rclone.exe"]

    def test_rclone_cmd_loaded_from_config_values(self):
        cfg = Config()
        _load_pull(cfg, lambda key, default="": {"RCLONE_CMD": " D:/tools/rclone.exe "}.get(key, default))
        assert cfg.rclone_cmd == "D:/tools/rclone.exe"




def _tree(tmp_path):
    base = tmp_path / "backups"
    (base / "postgres" / "app").mkdir(parents=True)
    (base / "postgres" / "app" / "a.dump").write_text("x")
    return base






def test_rclone_env_keeps_windows_system_vars_but_no_secrets(monkeypatch):
    monkeypatch.setenv("SystemRoot", "C:/Windows")
    monkeypatch.setenv("windir", "C:/Windows")
    monkeypatch.setenv("PGPASSWORD", "leak-me")
    env = storage.rclone_env()
    assert env["SystemRoot"] == "C:/Windows" and env["windir"] == "C:/Windows"
    assert "PGPASSWORD" not in env


class TestConfiguredRemote:
    def test_remote_keys_load(self):
        values = {"PULL_REMOTE": " akilimo-bkp:data ", "PUSH_REMOTE": "offsite:in"}
        cfg = Config()
        _load_pull(cfg, lambda key, default="": values.get(key, default))
        assert (cfg.pull_remote, cfg.push_remote) == ("akilimo-bkp:data", "offsite:in")

    def test_pull_uses_pull_remote_but_remote_flag_wins(self, monkeypatch, tmp_path):
        seen = []
        monkeypatch.setattr(pull, "rclone_available", lambda: True)
        monkeypatch.setattr(pull, "discover_jobs", lambda remote_path, *a, **kw: seen.append(remote_path) or [])
        cfg = Config()
        cfg.base_dir = str(tmp_path)
        cfg.pull_remote = "akilimo-bkp:data"
        with pytest.raises(SystemExit):  # no jobs -> exits, but after resolving the remote
            pull.run_pull(cfg)
        with pytest.raises(SystemExit):
            pull.run_pull(cfg, remote="other:x")
        assert seen == ["akilimo-bkp:data", "other:x"]

    def test_push_uses_push_remote(self, tmp_path, monkeypatch):
        cfg = Config()
        cfg.base_dir = str(_tree(tmp_path))
        cfg.push_remote = "offsite:in"
        calls = []
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage, "_upload", lambda base, remote, args, n: calls.append(remote))
        monkeypatch.setattr(storage, "_run_rclone", lambda args, timeout=0: (0, "", ""))
        run_sync(cfg, interactive=False, delete_local=False)
        assert calls == ["offsite:in"]




class TestPushRemote:
    def test_remote_flag_beats_push_remote(self, tmp_path, monkeypatch):
        cfg = Config()
        cfg.base_dir = str(_tree(tmp_path))
        cfg.push_remote = "offsite:in"
        calls = []
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage, "_upload", lambda base, remote, args, n: calls.append(remote))
        monkeypatch.setattr(storage, "_run_rclone", lambda args, timeout=0: (0, "", ""))
        run_sync(cfg, interactive=False, delete_local=False, remote="s3:bucket/x")
        assert calls == ["s3:bucket/x"]

    def test_push_init_saves_push_remote_only(self, tmp_path, monkeypatch):
        import fuelrod_backup.cli as cli_mod

        monkeypatch.setattr("fuelrod_backup.storage.rclone_available", lambda: False)
        out = tmp_path / ".backup"
        out.write_text("PULL_REMOTE=src:out\n")

        answers = iter(["offsite:incoming", ""])

        class Q:
            def text(self, msg, **kw):
                class A:
                    def ask(_self):
                        return next(answers)
                return A()

            def confirm(self, msg, **kw):
                class A:
                    def ask(_self):
                        return False
                return A()

        cli_mod._save_remote_target(Q(), out, "PUSH")
        text = out.read_text()
        assert "PUSH_REMOTE=offsite:incoming" in text and "PULL_REMOTE=src:out" in text
