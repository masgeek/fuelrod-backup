"""Tests for the doctor pre-flight checks."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from fuelrod_backup import doctor
from fuelrod_backup.config import Config, DbType
from fuelrod_backup.doctor import (
    Report,
    check_base_dir,
    check_binaries,
    check_credentials,
    check_retention,
    run_doctor,
)


def _cfg(**kw) -> Config:
    c = Config()
    c.db_type = DbType.POSTGRES
    c.user = "postgres"
    c.password = "secret"
    c.host = "127.0.0.1"
    c.port = 5432
    c.use_docker = False
    c.config_source = Path("C:/config/.backup")
    for k, v in kw.items():
        setattr(c, k, v)
    return c


def _names(report: Report) -> dict[str, str]:
    return {d.name: d.status for d in report.checks}


# ──────────────────────────────────────────────────────────────────────────────
#  Credentials
# ──────────────────────────────────────────────────────────────────────────────

class TestCheckCredentials:
    def test_all_present(self):
        r = Report()
        check_credentials(_cfg(), r)
        assert _names(r)["Credentials"] == "ok"

    @pytest.mark.parametrize("field", ["user", "password", "host", "port"])
    def test_missing_value_fails(self, field):
        r = Report()
        check_credentials(_cfg(**{field: ""}), r)
        diag = r.checks[0]
        assert diag.status == "fail"
        assert diag.hint


# ──────────────────────────────────────────────────────────────────────────────
#  BASE_DIR
# ──────────────────────────────────────────────────────────────────────────────

class TestCheckBaseDir:
    def test_unset_fails(self):
        r = Report()
        check_base_dir(_cfg(base_dir=""), r)
        assert _names(r)["BASE_DIR"] == "fail"

    def test_missing_directory_fails(self, tmp_path):
        r = Report()
        check_base_dir(_cfg(base_dir=str(tmp_path / "nope")), r)
        assert _names(r)["BASE_DIR"] == "fail"

    def test_existing_directory_adds_a_disk_space_check(self, tmp_path):
        r = Report()
        check_base_dir(_cfg(base_dir=str(tmp_path)), r)
        statuses = _names(r)
        assert statuses["BASE_DIR"] == "ok"
        assert statuses["Disk space"] in ("ok", "warn")


# ──────────────────────────────────────────────────────────────────────────────
#  Retention
# ──────────────────────────────────────────────────────────────────────────────

class TestCheckRetention:
    def test_negative_fails(self):
        r = Report()
        check_retention(_cfg(days_to_keep=-1), r)
        assert r.checks[0].status == "fail"

    def test_zero_warns(self):
        r = Report()
        check_retention(_cfg(days_to_keep=0), r)
        assert r.checks[0].status == "warn"

    def test_positive_ok(self):
        r = Report()
        check_retention(_cfg(days_to_keep=30), r)
        assert r.checks[0].status == "ok"


# ──────────────────────────────────────────────────────────────────────────────
#  Binaries
# ──────────────────────────────────────────────────────────────────────────────

class TestCheckBinaries:
    def test_docker_mode_skips_host_lookup(self):
        r = Report()
        check_binaries(_cfg(use_docker=True), r)
        assert r.checks[0].status == "ok"

    def test_missing_pg_dump_fails(self, monkeypatch):
        monkeypatch.setattr(shutil, "which", lambda _: None)
        r = Report()
        check_binaries(_cfg(), r)
        statuses = _names(r)
        assert statuses["Binary: pg_dump"] == "fail"
        assert statuses["Binary: pg_restore"] == "fail"
        assert statuses["Binary: psql"] == "skip"  # optional, never fatal

    def test_present_binaries_ok(self, monkeypatch):
        monkeypatch.setattr(shutil, "which", lambda b: f"/usr/bin/{b}")
        r = Report()
        check_binaries(_cfg(), r)
        assert all(status == "ok" for status in _names(r).values())

    def test_mariadb_binaries_checked(self, monkeypatch):
        monkeypatch.setattr(shutil, "which", lambda b: f"/usr/bin/{b}")
        r = Report()
        check_binaries(_cfg(db_type=DbType.MARIADB), r)
        assert "Binary: mariadb-dump" in _names(r)

    def test_mssql_needs_no_host_binaries(self):
        r = Report()
        check_binaries(_cfg(db_type=DbType.MSSQL), r)
        assert r.checks[0].status == "skip"


# ──────────────────────────────────────────────────────────────────────────────
#  Report aggregation + exit code
# ──────────────────────────────────────────────────────────────────────────────

class TestReport:
    def test_worst_status_wins(self):
        r = Report()
        r.add("a", "ok")
        r.add("b", "warn")
        r.add("c", "skip")
        assert r.worst() == "warn"

    def test_failures_and_warnings_are_separated(self):
        r = Report()
        r.add("a", "fail")
        r.add("b", "warn")
        r.add("c", "ok")
        assert len(r.failures) == 1
        assert len(r.warnings) == 1


class TestRunDoctor:
    def _patch(self, monkeypatch, tmp_path, **kw):
        monkeypatch.setattr(doctor, "check_connection", lambda cfg, report: report.add(
            "Database connection", "ok", "stubbed",
        ))
        monkeypatch.setattr(doctor, "check_docker", lambda cfg, report: None)
        monkeypatch.setattr(doctor, "check_rclone", lambda cfg, report: None)
        monkeypatch.setattr(doctor, "check_n8n_services", lambda cfg, report: None)
        monkeypatch.setattr(shutil, "which", lambda b: f"/usr/bin/{b}")
        kw.setdefault("base_dir", str(tmp_path))
        cfg = _cfg(**kw)
        return run_doctor(cfg)

    def test_clean_environment_exits_zero(self, monkeypatch, tmp_path):
        assert self._patch(monkeypatch, tmp_path) == 0

    def test_missing_base_dir_exits_one(self, monkeypatch, tmp_path):
        assert self._patch(monkeypatch, tmp_path, base_dir=str(tmp_path / "gone")) == 1

    def test_connection_failure_exits_one(self, monkeypatch, tmp_path):
        monkeypatch.setattr(doctor, "check_connection", lambda cfg, report: report.add(
            "Database connection", "fail", "refused",
        ))
        monkeypatch.setattr(doctor, "check_docker", lambda cfg, report: None)
        monkeypatch.setattr(doctor, "check_rclone", lambda cfg, report: None)
        monkeypatch.setattr(doctor, "check_n8n_services", lambda cfg, report: None)
        monkeypatch.setattr(shutil, "which", lambda b: f"/usr/bin/{b}")
        assert run_doctor(_cfg(base_dir=str(tmp_path))) == 1

    def test_skip_connection_omits_the_probe(self, monkeypatch, tmp_path):
        def boom(cfg, report):
            raise AssertionError("connection check must not run")

        monkeypatch.setattr(doctor, "check_connection", boom)
        monkeypatch.setattr(doctor, "check_docker", lambda cfg, report: None)
        monkeypatch.setattr(doctor, "check_rclone", lambda cfg, report: None)
        monkeypatch.setattr(doctor, "check_n8n_services", lambda cfg, report: None)
        monkeypatch.setattr(shutil, "which", lambda b: f"/usr/bin/{b}")
        assert run_doctor(_cfg(base_dir=str(tmp_path)), skip_connection=True) == 0
