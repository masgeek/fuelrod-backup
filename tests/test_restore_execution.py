"""Tests for pg_restore execution strategy."""

from __future__ import annotations

import gzip
from pathlib import Path

from fuelrod_backup.config import Config
from fuelrod_backup.restore import _execute_pg_restore_v2


def _write_gzip(path: Path, content: bytes) -> None:
    with gzip.open(path, "wb") as fh:
        fh.write(content)


def test_pg_restore_gz_uses_file_path_for_parallel_restore(tmp_path, monkeypatch):
    backup_file = tmp_path / "sample.dump.gz"
    _write_gzip(backup_file, b"pg dump bytes")

    calls: list[dict] = []

    def fake_run(cmd, **kwargs):
        calls.append({"cmd": cmd, "kwargs": kwargs})
        return None

    monkeypatch.setattr("fuelrod_backup.restore.subprocess.run", fake_run)

    cfg = Config(
        user="postgres",
        password="secret",
        host="127.0.0.1",
        port=5432,
        use_docker=False,
        pg_restore_cmd="pg_restore",
    )

    _execute_pg_restore_v2(backup_file, "fuelrod", ["-j", "4"], cfg)

    assert len(calls) == 1
    cmd = calls[0]["cmd"]
    kwargs = calls[0]["kwargs"]
    assert cmd[:2] == ["pg_restore", "-U"]
    assert "-j" in cmd
    assert "4" in cmd
    assert cmd[-1].endswith(".dump")
    assert not cmd[-1].endswith(".gz")
    assert "stdin" not in kwargs
    assert kwargs["env"]["PGPASSWORD"] == "secret"
    assert not Path(cmd[-1]).exists()


def test_pg_restore_docker_copies_temp_file_and_cleans_up(tmp_path, monkeypatch):
    backup_file = tmp_path / "sample.dump.gz"
    _write_gzip(backup_file, b"pg dump bytes")

    calls: list[dict] = []

    def fake_run(cmd, **kwargs):
        calls.append({"cmd": cmd, "kwargs": kwargs})
        return None

    monkeypatch.setattr("fuelrod_backup.restore.subprocess.run", fake_run)

    cfg = Config(
        user="postgres",
        password="secret",
        host="127.0.0.1",
        port=5432,
        service="postgres",
        use_docker=True,
        pg_restore_cmd="pg_restore",
    )

    _execute_pg_restore_v2(backup_file, "fuelrod", ["-j", "4"], cfg)

    assert len(calls) == 3
    assert calls[0]["cmd"][0:2] == ["docker", "cp"]
    copied_path = calls[0]["cmd"][2]
    container_path = calls[0]["cmd"][3].split(":", 1)[1]
    assert copied_path.endswith(".dump")
    assert not copied_path.endswith(".gz")
    assert calls[1]["cmd"][-1] == container_path
    assert calls[2]["cmd"] == ["docker", "exec", "postgres", "rm", "-f", container_path]
    assert calls[2]["kwargs"]["check"] is False
