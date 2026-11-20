"""Integration fixtures — run against real PostgreSQL / MariaDB servers.

Each engine is enabled by environment variables (set by the CI service containers);
without them every test here is skipped, so a plain ``pytest`` stays hermetic.

  IT_PG_HOST / IT_PG_PORT / IT_PG_USER / IT_PG_PASSWORD
  IT_MY_HOST / IT_MY_PORT / IT_MY_USER / IT_MY_PASSWORD
"""

from __future__ import annotations

import os
import uuid

import pytest

from fuelrod_backup.config import Config, DbType


def _engine_cfg(prefix: str, db_type: DbType, base_dir: str) -> Config:
    host = os.environ.get(f"IT_{prefix}_HOST")
    if not host:
        pytest.skip(f"IT_{prefix}_HOST not set — integration database unavailable")
    cfg = Config(
        user=os.environ.get(f"IT_{prefix}_USER", "root"),
        password=os.environ.get(f"IT_{prefix}_PASSWORD", ""),
        host=host,
        port=int(os.environ.get(f"IT_{prefix}_PORT", "0")),
        use_docker=False,
        base_dir=base_dir,
        db_type=db_type,
        connection_timeout=10,
    )
    if db_type == DbType.MARIADB:
        cfg.mysql_dump_cmd = os.environ.get("IT_MY_DUMP_CMD", "mariadb-dump")
        cfg.mysql_cmd = os.environ.get("IT_MY_CMD", "mariadb")
    return cfg


@pytest.fixture()
def pg_cfg(tmp_path) -> Config:
    return _engine_cfg("PG", DbType.POSTGRES, str(tmp_path))


@pytest.fixture()
def my_cfg(tmp_path) -> Config:
    return _engine_cfg("MY", DbType.MARIADB, str(tmp_path))


@pytest.fixture()
def db_name() -> str:
    return f"it_{uuid.uuid4().hex[:10]}"


@pytest.fixture()
def pg_source(pg_cfg, db_name):
    """A PostgreSQL database with one table of 3 rows; dropped afterwards."""
    import psycopg

    def connect(dbname: str):
        return psycopg.connect(
            host=pg_cfg.host, port=pg_cfg.port, user=pg_cfg.user, password=pg_cfg.password,
            dbname=dbname, autocommit=True,
        )

    with connect("postgres") as admin:
        admin.execute(f'CREATE DATABASE "{db_name}"')
    with connect(db_name) as conn:
        conn.execute("CREATE TABLE items (id serial PRIMARY KEY, name text NOT NULL)")
        conn.execute("INSERT INTO items (name) VALUES ('a'), ('b'), ('c')")
    yield db_name
    with connect("postgres") as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')


@pytest.fixture()
def my_source(my_cfg, db_name):
    """A MariaDB database with one table of 3 rows; dropped afterwards."""
    import pymysql

    def connect(dbname: str = ""):
        return pymysql.connect(
            host=my_cfg.host, port=my_cfg.port, user=my_cfg.user, password=my_cfg.password,
            database=dbname or None, autocommit=True,
        )

    admin = connect()
    try:
        with admin.cursor() as cur:
            cur.execute(f"CREATE DATABASE `{db_name}`")
    finally:
        admin.close()
    conn = connect(db_name)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE items (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20) NOT NULL)")
            cur.execute("INSERT INTO items (name) VALUES ('a'), ('b'), ('c')")
    finally:
        conn.close()
    yield db_name
    admin = connect()
    try:
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
    finally:
        admin.close()
