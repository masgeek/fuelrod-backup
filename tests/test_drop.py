"""Tests for the disconnect-only path in drop.py and the MSSQL connection killer."""

from unittest.mock import MagicMock

import pytest

from fuelrod_backup import drop as drop_mod
from fuelrod_backup.adapters.mssql import MssqlAdapter
from fuelrod_backup.config import Config, DbType
from fuelrod_backup.notify import EXIT_ABORTED


@pytest.fixture()
def adapter():
    """A stub adapter that records calls made by the disconnect wizard."""
    a = MagicMock()
    a.check_connection.return_value = None
    a.list_databases.return_value = ["app_db", "other_db"]
    a.get_table_count.return_value = "12"
    a.terminate_connections.return_value = 3
    return a


@pytest.fixture()
def cfg():
    c = Config()
    c.db_type = DbType.POSTGRES
    c.password = "pw"
    return c


@pytest.fixture(autouse=True)
def _no_prompt(monkeypatch):
    """Never hit the connection check or the name-confirmation prompt."""
    monkeypatch.setattr(drop_mod, "_check_connection", lambda cfg, adapter: None)


# ──────────────────────────────────────────────────────────────────────────────
#  _disconnect_database
# ──────────────────────────────────────────────────────────────────────────────

class TestDisconnectDatabase:
    def test_kills_connections_without_dropping(self, cfg, adapter, monkeypatch):
        monkeypatch.setattr(drop_mod, "get_adapter", lambda c: adapter)
        monkeypatch.setattr(drop_mod.questionary, "text", lambda *a, **k: MagicMock(ask=lambda: "app_db"))

        drop_mod._disconnect_database(cfg, adapter, db_name="app_db")

        adapter.terminate_connections.assert_called_once_with("app_db")
        adapter.drop_db.assert_not_called()
        adapter.drop_schema.assert_not_called()

    def test_explicit_db_skips_the_prompt(self, cfg, adapter, monkeypatch):
        monkeypatch.setattr(drop_mod, "get_adapter", lambda c: adapter)
        text_mock = MagicMock()
        text_mock.return_value.ask.return_value = "other_db"
        monkeypatch.setattr(drop_mod.questionary, "text", text_mock)
        select_mock = MagicMock()
        monkeypatch.setattr(drop_mod.questionary, "select", select_mock)

        drop_mod._disconnect_database(cfg, adapter, db_name="other_db")

        select_mock.assert_not_called()
        text_mock.assert_called_once()
        assert "other_db" in text_mock.call_args[0][0]

    def test_prompts_when_no_db_given(self, cfg, adapter, monkeypatch):
        select_mock = MagicMock()
        select_mock.return_value.ask.return_value = "app_db"
        monkeypatch.setattr(drop_mod.questionary, "select", select_mock)
        monkeypatch.setattr(drop_mod.questionary, "text", lambda *a, **k: MagicMock(ask=lambda: "app_db"))

        drop_mod._disconnect_database(cfg, adapter)

        select_mock.assert_called_once()
        adapter.terminate_connections.assert_called_once_with("app_db")

    def test_unknown_db_aborts(self, cfg, adapter, capsys):
        with pytest.raises(SystemExit) as exc:
            drop_mod._disconnect_database(cfg, adapter, db_name="ghost_db")
        assert exc.value.code == 1
        adapter.terminate_connections.assert_not_called()

    def test_wrong_confirmation_aborts(self, cfg, adapter, monkeypatch):
        monkeypatch.setattr(drop_mod.questionary, "text", lambda *a, **k: MagicMock(ask=lambda: "typo"))
        with pytest.raises(SystemExit) as exc:
            drop_mod._disconnect_database(cfg, adapter, db_name="app_db")
        assert exc.value.code == EXIT_ABORTED  # an operator abort is not success
        adapter.terminate_connections.assert_not_called()

    def test_kill_failure_is_reported(self, cfg, adapter, monkeypatch):
        monkeypatch.setattr(drop_mod.questionary, "text", lambda *a, **k: MagicMock(ask=lambda: "app_db"))
        adapter.terminate_connections.side_effect = RuntimeError("boom")
        with pytest.raises(SystemExit) as exc:
            drop_mod._disconnect_database(cfg, adapter, db_name="app_db")
        assert exc.value.code == 1


# ──────────────────────────────────────────────────────────────────────────────
#  run_drop dispatch
# ──────────────────────────────────────────────────────────────────────────────

class TestRunDropDispatch:
    def test_disconnect_only_short_circuits_the_drop_wizard(self, cfg, adapter, monkeypatch):
        monkeypatch.setattr(drop_mod, "get_adapter", lambda c: adapter)
        disconnect = MagicMock()
        monkeypatch.setattr(drop_mod, "_disconnect_database", disconnect)
        monkeypatch.setattr(drop_mod, "_drop_database", MagicMock())
        monkeypatch.setattr(drop_mod, "_drop_schema", MagicMock())

        drop_mod.run_drop(cfg, disconnect_only=True, db_name="app_db")

        disconnect.assert_called_once_with(cfg, adapter, "app_db")

    def test_drop_path_never_calls_disconnect(self, cfg, adapter, monkeypatch):
        monkeypatch.setattr(drop_mod, "get_adapter", lambda c: adapter)
        select_mock = MagicMock()
        select_mock.return_value.ask.return_value = "schema"
        monkeypatch.setattr(drop_mod.questionary, "select", select_mock)
        disconnect = MagicMock()
        monkeypatch.setattr(drop_mod, "_disconnect_database", disconnect)
        monkeypatch.setattr(drop_mod, "_drop_schema", MagicMock())
        monkeypatch.setattr(drop_mod, "_drop_database", MagicMock())

        drop_mod.run_drop(cfg)

        disconnect.assert_not_called()
        assert select_mock.call_args.kwargs["choices"][1].value == "connections"

    def test_database_still_the_default_target(self, cfg, adapter, monkeypatch):
        monkeypatch.setattr(drop_mod, "get_adapter", lambda c: adapter)
        select_mock = MagicMock()
        select_mock.return_value.ask.return_value = "database"
        monkeypatch.setattr(drop_mod.questionary, "select", select_mock)
        monkeypatch.setattr(drop_mod, "_disconnect_database", MagicMock())
        monkeypatch.setattr(drop_mod, "_drop_schema", MagicMock())
        drop_database = MagicMock()
        monkeypatch.setattr(drop_mod, "_drop_database", drop_database)

        drop_mod.run_drop(cfg)

        assert select_mock.call_args.kwargs["choices"][0].value == "database"
        drop_database.assert_called_once_with(cfg, adapter)


# ──────────────────────────────────────────────────────────────────────────────
#  MSSQL terminate_connections
# ──────────────────────────────────────────────────────────────────────────────

class TestMssqlTerminateConnections:
    def _adapter(self, spids):
        a = MssqlAdapter.__new__(MssqlAdapter)
        a._query_col = MagicMock(return_value=spids)
        a._execute = MagicMock()
        return a

    def test_returns_count_and_kills_every_spid(self):
        a = self._adapter(["55", "60", "61"])
        assert a.terminate_connections("app_db") == 3
        sql = a._execute.call_args[0][0]
        assert "KILL 55;" in sql and "KILL 60;" in sql and "KILL 61;" in sql

    def test_no_active_sessions_skips_the_kill(self):
        a = self._adapter([])
        assert a.terminate_connections("app_db") == 0
        a._execute.assert_not_called()

    def test_invalid_identifier_is_rejected(self):
        a = self._adapter(["55"])
        with pytest.raises(ValueError):
            a.terminate_connections("app; DROP DATABASE master--")
        a._execute.assert_not_called()
