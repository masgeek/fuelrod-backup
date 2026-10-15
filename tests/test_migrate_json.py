"""Tests for the migrate JSON payload and report serialisation."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from fuelrod_backup import output
from fuelrod_backup.config import Config, DbType
from fuelrod_backup.migrate import run_migrate
from fuelrod_backup.migrate.report import MigrationReport, TableResult


@pytest.fixture(autouse=True)
def _reset_mode():
    output._consoles.clear()
    output._json_mode = False
    yield
    output._consoles.clear()
    output._json_mode = False


def _cfg(db_type: DbType) -> Config:
    c = Config()
    c.db_type = db_type
    c.password = "pw"
    return c


def _report(*results: TableResult) -> MigrationReport:
    report = MigrationReport()
    for r in results:
        report.record(r)
    return report


# ──────────────────────────────────────────────────────────────────────────────
#  Report serialisation
# ──────────────────────────────────────────────────────────────────────────────

class TestReportToDict:
    def test_shape(self):
        report = _report(
            TableResult(table="users", status="ok", rows_source=10, rows_dest=10, count_match=True),
            TableResult(table="orders", status="failed", rows_source=5, error="boom"),
            TableResult(table="legacy", status="skipped"),
            TableResult(table="partial_t", status="partial", rows_skipped=2),
        )
        data = report.to_dict()

        assert {t["table"] for t in data["tables"]} == {"users", "orders", "legacy", "partial_t"}
        assert data["summary"] == {
            "total": 4, "ok": 1, "failed": 1, "partial": 1, "skipped": 1,
            "total_rows_migrated": 10, "total_rows_skipped": 2,
        }

    def test_write_json_matches_to_dict(self, tmp_path):
        report = _report(TableResult(table="users", status="ok"))
        out = tmp_path / "report.json"
        report.write_json(out)
        assert json.loads(out.read_text(encoding="utf-8")) == report.to_dict()


# ──────────────────────────────────────────────────────────────────────────────
#  run_migrate JSON
# ──────────────────────────────────────────────────────────────────────────────

class TestRunMigrateJson:
    def _patch(self, monkeypatch, report):
        monkeypatch.setattr(
            "fuelrod_backup.migrate.MigrateRunner",
            lambda **kwargs: MagicMock(migrate_database=lambda s, d: report),
        )

    def test_emits_report_with_source_and_target(self, monkeypatch, capsys):
        output.enable_json(True)
        report = _report(
            TableResult(table="users", status="ok", rows_dest=10, count_match=True),
        )
        self._patch(monkeypatch, report)

        run_migrate(
            _cfg(DbType.MARIADB),
            _cfg(DbType.POSTGRES),
            interactive=False,
            source_db="shop",
            target_db="shop_pg",
            target_schema="public",
        )

        payload = json.loads(capsys.readouterr().out)
        assert payload["command"] == "migrate"
        assert payload["status"] == "ok"
        assert payload["source"] == {"engine": "mariadb", "database": "shop"}
        assert payload["target"] == {
            "engine": "postgres", "database": "shop_pg", "schema": "public",
        }
        assert payload["summary"]["ok"] == 1
        assert payload["tables"][0]["table"] == "users"
        assert payload["dry_run"] is False

    def test_failures_exit_non_zero(self, monkeypatch, capsys):
        output.enable_json(True)
        report = _report(TableResult(table="orders", status="failed", error="boom"))
        self._patch(monkeypatch, report)

        with pytest.raises(SystemExit) as exc:
            run_migrate(
                _cfg(DbType.MARIADB), _cfg(DbType.POSTGRES),
                interactive=False, source_db="shop",
            )

        assert exc.value.code == 1
        assert json.loads(capsys.readouterr().out)["status"] == "fail"

    def test_migration_error_goes_to_stderr(self, monkeypatch, capsys):
        output.enable_json(True)
        runner = MagicMock()
        runner.migrate_database.side_effect = __import__(
            "fuelrod_backup.migrate.runner", fromlist=["MigrateError"]
        ).MigrateError("source unreachable")
        monkeypatch.setattr("fuelrod_backup.migrate.MigrateRunner", lambda **kw: runner)

        with pytest.raises(SystemExit):
            run_migrate(
                _cfg(DbType.MARIADB), _cfg(DbType.POSTGRES),
                interactive=False, source_db="shop",
            )

        captured = capsys.readouterr()
        assert captured.out == ""
        assert "source unreachable" in captured.err

    def test_missing_source_db_reported_on_stderr(self, capsys):
        output.enable_json(True)
        with pytest.raises(SystemExit):
            run_migrate(
                _cfg(DbType.MARIADB), _cfg(DbType.POSTGRES), interactive=False,
            )
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "--source-db is required" in captured.err

    def test_dry_run_is_flagged(self, monkeypatch, capsys):
        output.enable_json(True)
        report = _report(TableResult(table="users", status="ok"))
        self._patch(monkeypatch, report)

        run_migrate(
            _cfg(DbType.MARIADB), _cfg(DbType.POSTGRES),
            interactive=False, source_db="shop", dry_run=True,
        )

        assert json.loads(capsys.readouterr().out)["dry_run"] is True

    def test_report_file_path_is_included(self, monkeypatch, tmp_path, capsys):
        output.enable_json(True)
        report = _report(TableResult(table="users", status="ok"))
        self._patch(monkeypatch, report)
        out = tmp_path / "r.json"

        run_migrate(
            _cfg(DbType.MARIADB), _cfg(DbType.POSTGRES),
            interactive=False, source_db="shop", report_file=out,
        )

        assert json.loads(capsys.readouterr().out)["report_file"] == str(out)

    def test_human_mode_prints_the_summary_instead(self, monkeypatch, capsys):
        report = _report(TableResult(table="users", status="ok"))
        self._patch(monkeypatch, report)

        run_migrate(
            _cfg(DbType.MARIADB), _cfg(DbType.POSTGRES),
            interactive=False, source_db="shop",
        )

        out = capsys.readouterr().out
        assert "users" in out
        assert not out.lstrip().startswith("{")
