"""End-to-end backup / restore / drill against real database servers."""

from __future__ import annotations

from pathlib import Path

import pytest

from fuelrod_backup.adapters import get_adapter
from fuelrod_backup.backup import run_backup
from fuelrod_backup.catalog import list_backups
from fuelrod_backup.restoretest import run_restore_test

pytestmark = pytest.mark.integration


def _pg_count(cfg, dbname: str) -> int:
    import psycopg

    with psycopg.connect(
        host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, dbname=dbname
    ) as conn:
        return conn.execute("SELECT count(*) FROM items").fetchone()[0]


def _my_count(cfg, dbname: str) -> int:
    import pymysql

    conn = pymysql.connect(
        host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, database=dbname
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM items")
            return cur.fetchone()[0]
    finally:
        conn.close()


class TestPostgres:
    def test_connection_and_listing(self, pg_cfg, pg_source):
        adapter = get_adapter(pg_cfg)
        adapter.check_connection()
        assert pg_source in adapter.list_databases()

    def test_backup_then_catalog(self, pg_cfg, pg_source):
        run_backup(pg_cfg, interactive=False, databases=[pg_source])
        entries = list_backups(pg_cfg.base_dir, ["postgres"], pg_source)
        assert len(entries) == 1 and entries[0].size > 0

    def test_backup_and_restore_roundtrip(self, pg_cfg, pg_source, db_name):
        from fuelrod_backup.restore import _execute_pg_restore_v2

        adapter = get_adapter(pg_cfg)
        run_backup(pg_cfg, interactive=False, databases=[pg_source])
        dump = list_backups(pg_cfg.base_dir, ["postgres"], pg_source)[0].file

        target = f"{db_name}_restored"
        adapter.create_db(target)
        try:
            _execute_pg_restore_v2(Path(dump), target, ["--no-owner", "--no-privileges"], pg_cfg)
            assert _pg_count(pg_cfg, target) == 3
        finally:
            adapter.drop_db(target)

    def test_restore_drill_passes(self, pg_cfg, pg_source):
        run_backup(pg_cfg, interactive=False, databases=[pg_source])
        assert run_restore_test(pg_cfg, database=pg_source) == 0

    def test_active_connection_detected(self, pg_cfg, pg_source):
        import psycopg

        adapter = get_adapter(pg_cfg)
        assert adapter.active_connections(pg_source) == 0
        with psycopg.connect(
            host=pg_cfg.host, port=pg_cfg.port, user=pg_cfg.user, password=pg_cfg.password, dbname=pg_source
        ):
            assert adapter.active_connections(pg_source) >= 1


class TestMariaDb:
    def test_connection_and_listing(self, my_cfg, my_source):
        adapter = get_adapter(my_cfg)
        adapter.check_connection()
        assert my_source in adapter.list_databases()

    def test_backup_and_restore_roundtrip(self, my_cfg, my_source, db_name):
        adapter = get_adapter(my_cfg)
        run_backup(my_cfg, interactive=False, databases=[my_source])
        entries = list_backups(my_cfg.base_dir, ["mariadb"], my_source)
        assert len(entries) == 1 and entries[0].size > 0

        target = f"{db_name}_restored"
        adapter.create_db(target)
        try:
            adapter.restore_db(target, Path(entries[0].file), schemas=[], no_owner=True)
            assert _my_count(my_cfg, target) == 3
        finally:
            adapter.drop_db(target)

    def test_restore_drill_passes(self, my_cfg, my_source):
        run_backup(my_cfg, interactive=False, databases=[my_source])
        assert run_restore_test(my_cfg, database=my_source) == 0
