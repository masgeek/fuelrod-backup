"""Unit tests for MigrationReport — thread safety, JSON output, Rich summary."""

import json
import threading
from pathlib import Path

import pytest
from fuelrod_backup.migrate.report import MigrationReport, TableResult


def _make_result(table: str, status="ok", rows=10, failed=False) -> TableResult:
    return TableResult(
        table=table,
        status="failed" if failed else status,
        rows_source=rows,
        rows_dest=rows if not failed else 0,
        count_match=not failed,
        duration_s=1.0,
    )


class TestMigrationReport:
    def test_record_stores_result(self):
        report = MigrationReport()
        report.record(_make_result("users"))
        assert len(report.results) == 1
        assert report.results[0].table == "users"

    def test_has_failures_false_when_all_ok(self):
        report = MigrationReport()
        report.record(_make_result("a"))
        report.record(_make_result("b"))
        assert not report.has_failures()

    def test_has_failures_true_when_any_failed(self):
        report = MigrationReport()
        report.record(_make_result("a"))
        report.record(_make_result("b", failed=True))
        assert report.has_failures()

    def test_write_json_valid_json(self, tmp_path: Path):
        report = MigrationReport()
        report.record(_make_result("users", rows=100))
        report.record(_make_result("orders", rows=50))
        out = tmp_path / "report.json"
        report.write_json(out)
        data = json.loads(out.read_text())
        assert "tables" in data
        assert "summary" in data
        assert data["summary"]["total"] == 2
        assert data["summary"]["ok"] == 2

    def test_write_json_failed_table_counted(self, tmp_path: Path):
        report = MigrationReport()
        report.record(_make_result("a", failed=True))
        out = tmp_path / "report.json"
        report.write_json(out)
        data = json.loads(out.read_text())
        assert data["summary"]["failed"] == 1
        assert data["summary"]["ok"] == 0

    def test_thread_safe_concurrent_records(self):
        report = MigrationReport()
        threads = [
            threading.Thread(target=report.record, args=(_make_result(f"table_{i}"),))
            for i in range(50)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(report.results) == 50

    def test_print_summary_runs_without_error(self, capsys):
        from rich.console import Console
        from io import StringIO
        buf = StringIO()
        con = Console(file=buf, highlight=False)
        report = MigrationReport()
        report.record(_make_result("users", rows=42))
        report.record(_make_result("orders", failed=True))
        report.print_summary(con)
        output = buf.getvalue()
        assert "users" in output
        assert "orders" in output

    def test_partial_status_counted_separately(self, tmp_path: Path):
        report = MigrationReport()
        result = TableResult(table="t", status="partial", rows_source=100, rows_dest=90, rows_skipped=10)
        report.record(result)
        out = tmp_path / "report.json"
        report.write_json(out)
        data = json.loads(out.read_text())
        assert data["summary"]["partial"] == 1
