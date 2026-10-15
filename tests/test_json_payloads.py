"""Tests for JSON payloads on the create / n8n commands and stderr error routing."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from fuelrod_backup import create as create_mod
from fuelrod_backup import n8n_backup as n8n_backup_mod
from fuelrod_backup import n8n_restore as n8n_restore_mod
from fuelrod_backup import output
from fuelrod_backup.config import Config, DbType


@pytest.fixture(autouse=True)
def _reset_mode():
    output._consoles.clear()
    output._json_mode = False
    yield
    output._consoles.clear()
    output._json_mode = False


def _cfg(tmp_path) -> Config:
    c = Config()
    c.db_type = DbType.POSTGRES
    c.password = "pw"
    c.base_dir = str(tmp_path)
    return c


def _adapter(**kw) -> MagicMock:
    a = MagicMock()
    a.db_exists.return_value = False
    a.supports_schema_create = True
    for k, v in kw.items():
        setattr(a, k, v)
    return a


def _emit_calls(monkeypatch, module) -> list:
    """Capture a module's emit() — modules bind it at import, so patch there."""
    calls: list = []
    monkeypatch.setattr(module, "emit", lambda payload: calls.append(payload))
    return calls


# ──────────────────────────────────────────────────────────────────────────────
#  create
# ──────────────────────────────────────────────────────────────────────────────

class TestCreateJson:
    def test_database_payload(self, tmp_path, monkeypatch):
        output.enable_json(True)
        calls = _emit_calls(monkeypatch, create_mod)
        monkeypatch.setattr(create_mod, "get_adapter", lambda c: _adapter())
        monkeypatch.setattr(
            create_mod.questionary, "text", lambda *a, **k: MagicMock(ask=lambda: " app_db "))
        monkeypatch.setattr(
            create_mod.questionary, "confirm", lambda *a, **k: MagicMock(ask=lambda: True))

        create_mod._create_database(_cfg(tmp_path), _adapter())

        assert calls == [{
            "command": "create",
            "status": "ok",
            "action": "database",
            "engine": "postgres",
            "database": "app_db",
        }]

    def test_schema_payload(self, tmp_path, monkeypatch):
        output.enable_json(True)
        calls = _emit_calls(monkeypatch, create_mod)
        adapter = _adapter()
        adapter.list_databases.return_value = ["app_db"]
        adapter.get_user_schemas.return_value = []
        monkeypatch.setattr(
            create_mod.questionary, "select", lambda *a, **k: MagicMock(ask=lambda: "app_db"))
        monkeypatch.setattr(
            create_mod.questionary, "text", lambda *a, **k: MagicMock(ask=lambda: " reporting "))
        monkeypatch.setattr(
            create_mod.questionary, "confirm", lambda *a, **k: MagicMock(ask=lambda: True))

        create_mod._create_schema(_cfg(tmp_path), adapter)

        assert calls[0]["action"] == "schema"
        assert calls[0]["database"] == "app_db"
        assert calls[0]["schema"] == "reporting"

    def test_nothing_emitted_in_human_mode(self, tmp_path, monkeypatch):
        calls = _emit_calls(monkeypatch, create_mod)
        monkeypatch.setattr(
            create_mod.questionary, "text", lambda *a, **k: MagicMock(ask=lambda: "app_db"))
        monkeypatch.setattr(
            create_mod.questionary, "confirm", lambda *a, **k: MagicMock(ask=lambda: True))

        create_mod._create_database(_cfg(tmp_path), _adapter())

        assert calls == []


# ──────────────────────────────────────────────────────────────────────────────
#  n8n backup
# ──────────────────────────────────────────────────────────────────────────────

class TestN8nBackupJson:
    def test_records_are_collected(self, tmp_path, monkeypatch):
        output.enable_json(True)
        calls = _emit_calls(monkeypatch, n8n_backup_mod)
        cfg = _cfg(tmp_path)
        monkeypatch.setattr(n8n_backup_mod, "_ensure_alpine_image", lambda: None)
        monkeypatch.setattr(
            n8n_backup_mod, "_backup_service",
            lambda service, cfg: {
                "service": service, "status": "ok", "file": f"C:/backups/{service}.tar.gz",
                "size": "1.2 MB", "summary": None, "detail": "",
            },
        )

        n8n_backup_mod.run_n8n_backup(cfg, interactive=False, services=["n8n", "n8n-worker"])

        payload = calls[0]
        assert payload["command"] == "n8n-backup"
        assert [s["service"] for s in payload["services"]] == ["n8n", "n8n-worker"]
        assert payload["summary"] == {"total": 2, "ok": 2, "skipped": 0}

    def test_skipped_service_is_reported(self, tmp_path, monkeypatch):
        output.enable_json(True)
        calls = _emit_calls(monkeypatch, n8n_backup_mod)
        cfg = _cfg(tmp_path)
        monkeypatch.setattr(n8n_backup_mod, "_ensure_alpine_image", lambda: None)
        monkeypatch.setattr(
            n8n_backup_mod, "_backup_service",
            lambda service, cfg: {
                "service": service, "status": "skipped", "file": None, "size": None,
                "summary": None, "detail": "container not running",
            },
        )

        n8n_backup_mod.run_n8n_backup(cfg, interactive=False, services=["n8n"])

        assert calls[0]["summary"]["skipped"] == 1
        assert calls[0]["services"][0]["detail"] == "container not running"

    def test_backup_service_returns_a_record_when_not_running(self, tmp_path, monkeypatch):
        monkeypatch.setattr(n8n_backup_mod, "_is_container_running", lambda s: False)
        record = n8n_backup_mod._backup_service("n8n", _cfg(tmp_path))
        assert record["status"] == "skipped"
        assert record["file"] is None


# ──────────────────────────────────────────────────────────────────────────────
#  n8n restore
# ──────────────────────────────────────────────────────────────────────────────

class TestN8nRestoreJson:
    def test_dry_run_payload(self, tmp_path, monkeypatch, capsys):
        output.enable_json(True)
        archive = tmp_path / "n8n_hot_backup_20240101_120000_123.tar.gz"
        archive.write_bytes(b"tar")
        cfg = _cfg(tmp_path)
        monkeypatch.setattr(n8n_restore_mod, "_confirm_restore", lambda *a, **k: None)

        n8n_restore_mod.run_n8n_restore(
            cfg, service="n8n", backup_file=archive, dry_run=True)

        assert json.loads(capsys.readouterr().out) == {
            "command": "n8n-restore",
            "status": "dry_run",
            "service": "n8n",
            "volume": "n8n-data",
            "backup_file": str(archive),
            "executed": False,
        }

    def test_service_and_file_are_mandatory_in_json_mode(self, tmp_path):
        output.enable_json(True)
        with pytest.raises(SystemExit):
            n8n_restore_mod.run_n8n_restore(_cfg(tmp_path), service="n8n")

    def test_missing_target_is_reported_on_stderr(self, tmp_path, capsys):
        output.enable_json(True)
        with pytest.raises(SystemExit):
            n8n_restore_mod.run_n8n_restore(_cfg(tmp_path))
        assert "ERROR:" in capsys.readouterr().err
        assert capsys.readouterr().out == ""


# ──────────────────────────────────────────────────────────────────────────────
#  Errors must survive JSON mode
# ──────────────────────────────────────────────────────────────────────────────

class TestErrorsInJsonMode:
    @pytest.mark.parametrize("module", [
        create_mod, n8n_backup_mod, n8n_restore_mod,
    ])
    def test_die_writes_to_stderr(self, module, capsys):
        output.enable_json(True)
        with pytest.raises(SystemExit) as exc:
            module._die("something went wrong")
        captured = capsys.readouterr()
        assert "ERROR: something went wrong" in captured.err
        assert captured.out == "", "stdout must stay clean for the JSON document"
        assert exc.value.code == 1

    def test_output_error_helper(self, capsys):
        output.error("boom")
        assert "ERROR: boom" in capsys.readouterr().err
