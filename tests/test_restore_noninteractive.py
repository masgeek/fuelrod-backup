"""Tests for non-interactive restore target resolution."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import click
import pytest

from fuelrod_backup.config import Config, DbType
from fuelrod_backup.restore import (
    _list_backup_files,
    _prepare_target_db,
    _resolve_backup_file,
)


def _make_backups(db_dir: Path, names: list[str]) -> None:
    """Create backup files whose mtimes increase with their order in *names*."""
    db_dir.mkdir(parents=True, exist_ok=True)
    for i, name in enumerate(names):
        f = db_dir / name
        f.write_text(f"payload {i}")
        os.utime(f, (1_700_000_000 + i, 1_700_000_000 + i))


# ──────────────────────────────────────────────────────────────────────────────
#  _list_backup_files
# ──────────────────────────────────────────────────────────────────────────────

class TestListBackupFiles:
    def test_orders_oldest_first(self, tmp_path):
        _make_backups(tmp_path, ["a.dump", "b.sql.gz", "c.bak"])
        files = _list_backup_files(tmp_path)
        assert [f.name for f in files] == ["a.dump", "b.sql.gz", "c.bak"]

    def test_ignores_unrelated_files(self, tmp_path):
        _make_backups(tmp_path, ["a.dump"])
        (tmp_path / "notes.txt").write_text("ignore me")
        assert [f.name for f in _list_backup_files(tmp_path)] == ["a.dump"]

    def test_empty_dir(self, tmp_path):
        assert _list_backup_files(tmp_path) == []


# ──────────────────────────────────────────────────────────────────────────────
#  _resolve_backup_file
# ──────────────────────────────────────────────────────────────────────────────

class TestResolveBackupFile:
    def test_defaults_to_latest_by_mtime(self, tmp_path):
        _make_backups(tmp_path, ["old.dump", "new.dump", "mid.dump"])
        assert _resolve_backup_file(tmp_path, "app", None).name == "mid.dump"

    def test_explicit_name_wins(self, tmp_path):
        _make_backups(tmp_path, ["old.dump", "new.dump"])
        assert _resolve_backup_file(tmp_path, "app", "old.dump").name == "old.dump"

    def test_missing_name_aborts(self, tmp_path):
        _make_backups(tmp_path, ["new.dump"])
        with pytest.raises(SystemExit):
            _resolve_backup_file(tmp_path, "app", "ghost.dump")

    @pytest.mark.parametrize("evil", ["../escape.dump", "sub/child.dump", "..", "..\\escape.dump"])
    def test_path_traversal_rejected(self, tmp_path, evil):
        _make_backups(tmp_path, ["new.dump"])
        with pytest.raises(SystemExit):
            _resolve_backup_file(tmp_path, "app", evil)

    def test_empty_dir_aborts(self, tmp_path):
        tmp_path.mkdir(exist_ok=True)
        with pytest.raises(SystemExit):
            _resolve_backup_file(tmp_path, "app", None)


# ──────────────────────────────────────────────────────────────────────────────
#  _prepare_target_db
# ──────────────────────────────────────────────────────────────────────────────

class TestPrepareTargetDb:
    def _adapter(self, exists: bool):
        a = MagicMock()
        a.db_exists.return_value = exists
        a.terminate_connections.return_value = 2
        return a

    def test_creates_missing_database(self):
        a = self._adapter(exists=False)
        _prepare_target_db("app", a, drop_existing=True)
        a.create_db.assert_called_once_with("app")
        a.drop_db.assert_not_called()

    def test_drops_then_creates_when_requested(self):
        a = self._adapter(exists=True)
        _prepare_target_db("app", a, drop_existing=True)
        a.terminate_connections.assert_called_once_with("app")
        a.drop_db.assert_called_once_with("app")
        a.create_db.assert_called_once_with("app")

    def test_overlays_when_not_dropping(self):
        a = self._adapter(exists=True)
        _prepare_target_db("app", a, drop_existing=False)
        a.drop_db.assert_not_called()
        a.create_db.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
#  CLI wiring
# ──────────────────────────────────────────────────────────────────────────────

def _cfg() -> Config:
    c = Config()
    c.db_type = DbType.POSTGRES
    c.password = "pw"
    c.base_dir = ""
    return c


class TestNonInteractiveRestore:
    def _patch(self, monkeypatch, **kwargs):
        import fuelrod_backup.restore as restore_mod
        calls = []

        def fake_run_restore(cfg, **kw):
            calls.append(kw)

        monkeypatch.setattr(restore_mod, "run_restore", fake_run_restore)
        return calls

    def test_requires_project(self, monkeypatch, tmp_path):
        import fuelrod_backup.cli as cli_mod
        cfg = _cfg()
        cfg.base_dir = str(tmp_path)
        monkeypatch.setattr(cli_mod, "load_config", lambda *a, **k: cfg)
        with pytest.raises(SystemExit):
            cli_mod.restore(no_interactive=True, databases=["app"])

    def test_requires_db(self, monkeypatch, tmp_path):
        import fuelrod_backup.cli as cli_mod
        cfg = _cfg()
        cfg.base_dir = str(tmp_path)
        monkeypatch.setattr(cli_mod, "load_config", lambda *a, **k: cfg)
        with pytest.raises((SystemExit, click.exceptions.Exit)):
            cli_mod.restore(no_interactive=True, project="proj")

    def test_plan_only_without_yes(self, monkeypatch, tmp_path):
        import fuelrod_backup.cli as cli_mod
        cfg = _cfg()
        cfg.base_dir = str(tmp_path)
        monkeypatch.setattr(cli_mod, "load_config", lambda *a, **k: cfg)
        calls = self._patch(monkeypatch)
        cli_mod.restore(no_interactive=True, project="proj", databases=["app"], yes=False)
        assert calls[0]["interactive"] is False
        assert calls[0]["yes"] is False
        assert calls[0]["database"] == "app"

    def test_multiple_databases_restore_each(self, monkeypatch, tmp_path):
        import fuelrod_backup.cli as cli_mod
        cfg = _cfg()
        cfg.base_dir = str(tmp_path)
        monkeypatch.setattr(cli_mod, "load_config", lambda *a, **k: cfg)
        calls = self._patch(monkeypatch)
        cli_mod.restore(
            no_interactive=True, project="proj", databases=["one", "two"], yes=True
        )
        assert [c["database"] for c in calls] == ["one", "two"]

    def test_interactive_rejects_multiple_databases(self, monkeypatch, tmp_path):
        import fuelrod_backup.cli as cli_mod
        cfg = _cfg()
        cfg.base_dir = str(tmp_path)
        monkeypatch.setattr(cli_mod, "load_config", lambda *a, **k: cfg)
        with pytest.raises((SystemExit, click.exceptions.Exit)):
            cli_mod.restore(databases=["one", "two"])
