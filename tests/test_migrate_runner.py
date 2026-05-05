"""Integration-style tests for MigrateRunner with monkeypatched adapters."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fuelrod_backup.config import Config, DbType
from fuelrod_backup.migrate.runner import MigrateRunner
from fuelrod_backup.migrate.schema import ColumnDef, DatabaseSchema, TableDef


def _src_cfg():
    cfg = Config()
    cfg.db_type = DbType.MARIADB
    cfg.user = "root"
    cfg.password = "secret"
    cfg.host = "127.0.0.1"
    cfg.port = 3306
    cfg.use_docker = False
    return cfg


def _dst_cfg():
    cfg = Config()
    cfg.db_type = DbType.POSTGRES
    cfg.user = "postgres"
    cfg.password = "secret"
    cfg.host = "127.0.0.1"
    cfg.port = 5432
    cfg.use_docker = False
    return cfg


def _simple_table():
    return TableDef(
        name="users",
        columns=[
            ColumnDef("id", 1, "int", "int(11)", False, False, None, "auto_increment", "PRI", "", None, None, None, None),
            ColumnDef("name", 2, "varchar", "varchar(100)", True, False, None, "", "", "", 100, None, None, None),
        ],
    )


def _simple_schema():
    return DatabaseSchema(name="mydb", tables=[_simple_table()])


def _make_runner(dry_run=False, validate=False, fail_fast=False, **kwargs):
    runner = MigrateRunner(
        src_cfg=_src_cfg(),
        dst_cfg=_dst_cfg(),
        dry_run=dry_run,
        validate=validate,
        fail_fast=fail_fast,
        **kwargs,
    )
    return runner


class TestDryRun:
    def test_dry_run_writes_nothing(self, monkeypatch, tmp_path):
        runner = _make_runner(dry_run=True)

        schema = _simple_schema()
        monkeypatch.setattr(
            "fuelrod_backup.migrate.runner.SchemaExtractor.extract",
            lambda self, dbname: schema,
        )
        monkeypatch.setattr(
            "fuelrod_backup.migrate.runner.MigrateRunner._ensure_target_db",
            lambda self, dst_db: None,
        )
        execute_calls = []
        monkeypatch.setattr(
            "fuelrod_backup.migrate.runner.PgRunner._execute",
            lambda self, sql, dbname="": execute_calls.append(sql),
        )

        report = runner.migrate_database("mydb", "mydb_pg")
        # No table results should be recorded in dry-run
        assert len(report.results) == 0
        # No DDL execute calls
        assert not execute_calls


class TestTableFailure:
    def test_table_failure_recorded_but_others_continue(self, monkeypatch):
        from fuelrod_backup.migrate.schema import ColumnDef, TableDef, DatabaseSchema

        table_a = TableDef("a", columns=[
            ColumnDef("id", 1, "int", "int(11)", False, False, None, "", "PRI", "", None, None, None, None),
        ])
        table_b = TableDef("b", columns=[
            ColumnDef("id", 1, "int", "int(11)", False, False, None, "", "PRI", "", None, None, None, None),
        ])
        schema = DatabaseSchema(name="db", tables=[table_a, table_b])

        runner = _make_runner(validate=False, parallel=1)

        monkeypatch.setattr(
            "fuelrod_backup.migrate.runner.SchemaExtractor.extract",
            lambda self, dbname: schema,
        )
        monkeypatch.setattr(
            "fuelrod_backup.migrate.runner.MigrateRunner._ensure_target_db",
            lambda self, dst_db: None,
        )
        monkeypatch.setattr(
            "fuelrod_backup.migrate.runner.PgRunner._execute",
            lambda self, sql, dbname="": None,
        )

        call_count = {"n": 0}

        def _fake_migrate_table(self, table, columns, src_cfg, dst_runner, src_db, dst_db,
                                target_schema="public", batch_size=1000, skipped_log=None):
            from fuelrod_backup.migrate.report import TableResult
            call_count["n"] += 1
            if table == "a":
                return TableResult(table="a", status="failed", error="injected failure")
            return TableResult(table="b", status="ok", rows_source=5, rows_dest=5, count_match=True)

        monkeypatch.setattr(
            "fuelrod_backup.migrate.runner.DataMigrator.migrate_table",
            _fake_migrate_table,
        )

        report = runner.migrate_database("db", "db_pg")
        assert call_count["n"] == 2  # both tables attempted
        assert report.has_failures()
        ok_tables = [r for r in report.results if r.status == "ok"]
        assert len(ok_tables) == 1
        assert ok_tables[0].table == "b"

    def test_fail_fast_stops_on_first_failure(self, monkeypatch):
        from fuelrod_backup.migrate.schema import DatabaseSchema, TableDef, ColumnDef

        tables = [
            TableDef(f"t{i}", columns=[ColumnDef("id", 1, "int", "int(11)", False, False, None, "", "PRI", "", None, None, None, None)])
            for i in range(5)
        ]
        schema = DatabaseSchema(name="db", tables=tables)

        runner = _make_runner(validate=False, fail_fast=True, parallel=1)

        monkeypatch.setattr("fuelrod_backup.migrate.runner.SchemaExtractor.extract", lambda s, d: schema)
        monkeypatch.setattr("fuelrod_backup.migrate.runner.MigrateRunner._ensure_target_db", lambda s, d: None)
        monkeypatch.setattr("fuelrod_backup.migrate.runner.PgRunner._execute", lambda s, sql, dbname="": None)

        attempted = {"n": 0}

        def _fake(self, table, columns, src_cfg, dst_runner, src_db, dst_db,
                  target_schema="public", batch_size=1000, skipped_log=None):
            from fuelrod_backup.migrate.report import TableResult
            attempted["n"] += 1
            return TableResult(table=table, status="failed", error="fail")

        monkeypatch.setattr("fuelrod_backup.migrate.runner.DataMigrator.migrate_table", _fake)
        report = runner.migrate_database("db", "db_pg")
        assert attempted["n"] < 5, "fail_fast should have stopped before migrating all tables"
