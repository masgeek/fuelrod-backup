"""Tests for PostgreSQL incremental backup bookkeeping."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from fuelrod_backup.backup import (
    _backup_one,
    _cleanup_old,
    _load_incremental_state,
    _protected_bases,
    changed_tables,
    incremental_chain,
    prune_incremental,
)
from fuelrod_backup.config import Config, DbType


def _write(path: Path, mtime: int, data: bytes = b"dump") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    os.utime(path, (mtime, mtime))
    return path


def _state(db_dir: Path, state: dict) -> None:
    (db_dir / ".incremental_state.json").write_text(json.dumps(state), encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────────
#  changed_tables
# ──────────────────────────────────────────────────────────────────────────────

class TestChangedTables:
    def test_new_table_is_included(self):
        assert changed_tables({}, {"users": 10}) == ["users"]

    def test_moved_counter_is_included(self):
        assert changed_tables({"users": 10}, {"users": 42}) == ["users"]

    def test_unchanged_table_is_skipped(self):
        assert changed_tables({"users": 10}, {"users": 10}) == []

    def test_dropped_table_is_not_dumped(self):
        # There is nothing to dump for a vanished table; its rows stay in the base.
        assert changed_tables({"users": 10, "orders": 5}, {"users": 10}) == []

    def test_mixed(self):
        assert changed_tables(
            {"a": 1, "b": 2, "c": 3},
            {"a": 1, "b": 9, "d": 4},
        ) == ["b", "d"]


# ──────────────────────────────────────────────────────────────────────────────
#  State sidecar
# ──────────────────────────────────────────────────────────────────────────────

class TestState:
    def test_missing_state(self, tmp_path):
        assert _load_incremental_state(tmp_path) is None

    def test_valid_state_round_trips(self, tmp_path):
        _write(tmp_path / "app_20260101_000000.dump", 100)
        _state(tmp_path, {"base": "app_20260101_000000.dump", "signature": {"t": 1}})
        loaded = _load_incremental_state(tmp_path)
        assert loaded["signature"] == {"t": 1}

    def test_corrupt_json_is_ignored(self, tmp_path):
        (tmp_path / ".incremental_state.json").write_text("{not json", encoding="utf-8")
        assert _load_incremental_state(tmp_path) is None

    def test_state_without_signature_is_ignored(self, tmp_path):
        _state(tmp_path, {"base": "app.dump"})
        assert _load_incremental_state(tmp_path) is None

    def test_vanished_base_forces_a_full_dump(self, tmp_path):
        # The increments on disk are worthless without their base.
        _state(tmp_path, {"base": "deleted.dump", "signature": {"t": 1}})
        assert _load_incremental_state(tmp_path) is None


# ──────────────────────────────────────────────────────────────────────────────
#  Pruning and chain resolution
# ──────────────────────────────────────────────────────────────────────────────

class TestPruneAndChain:
    def _chain_files(self, tmp_path) -> None:
        _write(tmp_path / "app_20260101_000000.dump", 100)
        for i, ts in enumerate((200, 300, 400)):
            _write(tmp_path / f"app_20260102_00000{i}_inc.dump", ts)

    def test_prune_keeps_newest(self, tmp_path):
        self._chain_files(tmp_path)
        removed = prune_incremental(tmp_path, keep=2)
        assert [p.name for p in removed] == ["app_20260102_000000_inc.dump"]
        assert (tmp_path / "app_20260101_000000.dump").exists()  # base untouched

    def test_prune_never_removes_the_base(self, tmp_path):
        _write(tmp_path / "app_20260101_000000.dump", 100)
        assert prune_incremental(tmp_path, keep=0) == []
        assert (tmp_path / "app_20260101_000000.dump").exists()

    def test_chain_lists_newer_increments_oldest_first(self, tmp_path):
        self._chain_files(tmp_path)
        chain = incremental_chain(tmp_path, tmp_path / "app_20260101_000000.dump")
        assert [p.name for p in chain] == [
            "app_20260102_000000_inc.dump",
            "app_20260102_000001_inc.dump",
            "app_20260102_000002_inc.dump",
        ]

    def test_chain_excludes_older_increments(self, tmp_path):
        self._chain_files(tmp_path)
        base = tmp_path / "app_20260102_000002_inc.dump"
        assert incremental_chain(tmp_path, base) == []

    def test_protected_bases_blocks_age_pruning(self, tmp_path):
        db_dir = tmp_path / "app"
        _write(db_dir / "app_20260101_000000.dump", 1)  # ancient
        _state(db_dir, {"base": "app_20260101_000000.dump", "signature": {}})
        protected = _protected_bases(tmp_path)
        assert protected == {"app_20260101_000000.dump"}
        _cleanup_old(str(tmp_path), days=1, protect=protected)
        assert (db_dir / "app_20260101_000000.dump").exists()


# ──────────────────────────────────────────────────────────────────────────────
#  _backup_one behaviour
# ──────────────────────────────────────────────────────────────────────────────

def _adapter(signature: dict[str, int], *, supports_incremental: bool = True) -> MagicMock:
    a = MagicMock()
    a.supports_incremental = supports_incremental
    a.dump_extension = ".dump"
    a.table_change_signature.side_effect = lambda db: dict(signature)
    a.backup_db.side_effect = lambda db, out, **kw: out.write_bytes(b"full")
    a.backup_tables.side_effect = lambda db, out, tables: out.write_bytes(b"inc")
    return a


def _cfg(tmp_path: Path) -> Config:
    c = Config()
    c.db_type = DbType.POSTGRES
    c.user = "postgres"
    c.password = "pw"
    c.base_dir = str(tmp_path)
    c.incremental = True
    return c


class TestBackupOne:
    def test_first_run_writes_a_full_dump_and_state(self, tmp_path):
        adapter = _adapter({"users": 10})
        cfg = _cfg(tmp_path)
        out = _backup_one("app", cfg, adapter)

        assert out is not None and out.name.endswith(".dump") and "_inc" not in out.name
        adapter.backup_tables.assert_not_called()
        state = _load_incremental_state(tmp_path / "postgres" / "app")
        assert state["base"] == out.name
        assert state["signature"] == {"users": 10}

    def test_second_run_with_no_changes_writes_nothing(self, tmp_path):
        cfg = _cfg(tmp_path)
        adapter = _adapter({"users": 10})
        _backup_one("app", cfg, adapter)

        assert _backup_one("app", cfg, _adapter({"users": 10})) is None

    def test_changed_tables_produce_an_increment(self, tmp_path):
        cfg = _cfg(tmp_path)
        base = _backup_one("app", cfg, _adapter({"users": 10, "orders": 5}))
        adapter = _adapter({"users": 10, "orders": 9})
        inc = _backup_one("app", cfg, adapter)

        assert inc is not None and "_inc.dump" in inc.name
        adapter.backup_tables.assert_called_once_with("app", inc, ["orders"])
        # The base of the chain must not be replaced by the increment.
        state = _load_incremental_state(tmp_path / "postgres" / "app")
        assert state["base"] == base.name

    def test_incremental_disabled_always_takes_a_full_dump(self, tmp_path):
        cfg = _cfg(tmp_path)
        cfg.incremental = False
        _backup_one("app", cfg, _adapter({"users": 10}))
        out = _backup_one("app", cfg, _adapter({"users": 10}))

        assert out is not None and "_inc" not in out.name

    def test_incremental_off_for_unsupported_engines(self, tmp_path):
        cfg = _cfg(tmp_path)
        adapter = _adapter({}, supports_incremental=False)
        out = _backup_one("app", cfg, adapter)

        assert out is not None and "_inc" not in out.name
        assert _load_incremental_state(tmp_path / "postgres" / "app") is None

    def test_compressed_increment_keeps_its_suffix(self, tmp_path):
        cfg = _cfg(tmp_path)
        cfg.compress = True
        _backup_one("app", cfg, _adapter({"users": 10}))
        inc = _backup_one("app", cfg, _adapter({"users": 11}))

        assert inc is not None and inc.name.endswith("_inc.dump.gz")

    def test_manifest_records_the_base_and_tables(self, tmp_path):
        cfg = _cfg(tmp_path)
        _backup_one("app", cfg, _adapter({"users": 10}))
        _backup_one("app", cfg, _adapter({"users": 11}))
        manifest = sorted((tmp_path / "postgres" / "app").glob("manifest_*.txt"))[-1].read_text()

        assert "Mode      : incremental" in manifest
        assert "Tables    : 1 changed" in manifest
        assert "  - users" in manifest

    @pytest.mark.parametrize("bad", ["../evil", "a/b", "..\\evil"])
    def test_database_name_is_still_validated(self, tmp_path, bad):
        cfg = _cfg(tmp_path)
        with pytest.raises(SystemExit):
            _backup_one(bad, cfg, _adapter({}))
