"""Tests for pull --exclude and for removing empty folders."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from fuelrod_backup import pull, pullhistory, storage
from fuelrod_backup.config import Config, _load_pull
from fuelrod_backup.fileutil import remove_empty_dirs
from fuelrod_backup.pull import _exclude_list
from tests.conftest import patch_rclone
from tests.test_manifest import _manifest_of
from tests.test_pull import FakeRemote, _cfg, _pull


class TestRemoveEmptyDirs:
    def test_nested_empty_folders_go_but_the_root_stays(self, tmp_path):
        (tmp_path / "postgres" / "app").mkdir(parents=True)
        (tmp_path / "mariadb" / "shop" / "old").mkdir(parents=True)
        gone = remove_empty_dirs(tmp_path)
        assert tmp_path.is_dir() and list(tmp_path.iterdir()) == []
        assert len(gone) == 5

    def test_folders_with_files_stay_even_hidden_ones(self, tmp_path):
        (tmp_path / "a" / "b").mkdir(parents=True)
        (tmp_path / "a" / "b" / "x.dump").write_text("x")
        (tmp_path / "c").mkdir()
        (tmp_path / "c" / ".incremental_state.json").write_text("{}")
        (tmp_path / "d").mkdir()
        remove_empty_dirs(tmp_path)
        assert (tmp_path / "a" / "b" / "x.dump").exists()
        assert (tmp_path / "c").exists() and not (tmp_path / "d").exists()

    def test_missing_root_is_fine(self, tmp_path):
        assert remove_empty_dirs(tmp_path / "nope") == []

    def test_push_with_local_deletion_leaves_no_empty_folders(self, tmp_path, monkeypatch):
        base = tmp_path / "backups"
        (base / "postgres" / "app").mkdir(parents=True)
        (base / "postgres" / "app" / "app_1.dump").write_text("x")
        cfg = Config()
        cfg.base_dir = str(base)
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage, "_upload", lambda *a, **k: None)
        patch_rclone(monkeypatch, lambda args, timeout=0: (0, "[]", ""))
        storage.run_sync(cfg, remote="s3:b/x", interactive=False, delete_local=True)
        assert base.is_dir() and list(base.rglob("*.dump")) == []
        assert [p for p in base.iterdir() if p.is_dir()] == []  # engine/database folders are gone too

    def test_keep_local_keeps_the_folders(self, tmp_path, monkeypatch):
        base = tmp_path / "backups"
        (base / "postgres" / "app").mkdir(parents=True)
        (base / "postgres" / "app" / "app_1.dump").write_text("x")
        cfg = Config()
        cfg.base_dir = str(base)
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage, "_upload", lambda *a, **k: None)
        patch_rclone(monkeypatch, lambda args, timeout=0: (0, "[]", ""))
        storage.run_sync(cfg, remote="s3:b/x", interactive=False, delete_local=False)
        assert (base / "postgres" / "app" / "app_1.dump").exists()

    def test_age_prune_on_the_destination_also_removes_emptied_folders(self, tmp_path, monkeypatch):
        base = tmp_path / "backups"
        (base / "postgres" / "app").mkdir(parents=True)
        (base / "postgres" / "app" / "app_1.dump").write_text("x")
        cfg = Config()
        cfg.base_dir = str(base)
        calls = []
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage, "_upload", lambda *a, **k: None)
        patch_rclone(monkeypatch, lambda args, timeout=0: (0, "[]", ""))
        monkeypatch.setattr(storage, "_run_rclone", lambda args, timeout=0: calls.append(args) or (0, "[]", ""))
        storage.run_sync(cfg, remote="s3:b/x", interactive=False, delete_local=False, age_days=30)
        prune = next(c for c in calls if c[0] == "delete")
        assert "--rmdirs" in prune


@pytest.fixture
def remote(monkeypatch, tmp_path):
    fake = FakeRemote()
    monkeypatch.setattr(pull.subprocess, "run", fake)
    monkeypatch.setattr(pull, "rclone_available", lambda: True)
    monkeypatch.setattr(pullhistory, "default_path", lambda: tmp_path / "history.json")
    return fake


def _fetched(remote):
    return sorted(name for _s, _d, names in remote.copies for name in names)


class TestExclude:
    def test_file_name_glob(self, remote, tmp_path):
        _pull(tmp_path, engines=["postgres"], databases=["app"], exclude_patterns=["app_2*"])
        assert _fetched(remote) == ["app_1.dump", "app_3.dump"]

    def test_full_path_glob_skips_a_whole_database(self, remote, tmp_path):
        _pull(tmp_path, exclude_patterns=["postgres/billing/*"])
        assert "bill_1.dump" not in _fetched(remote) and "app_1.dump" in _fetched(remote)

    def test_several_patterns_and_extension(self, remote, tmp_path):
        _pull(tmp_path, exclude_patterns=["*.sql", "*_3.dump"])
        assert _fetched(remote) == ["app_1.dump", "app_2.dump", "bill_1.dump"]

    def test_excluded_files_are_not_recorded_deleted_or_counted_as_pulled(self, remote, tmp_path):
        _pull(tmp_path, engines=["postgres"], databases=["app"], exclude_patterns=["app_1.dump"],
              delete_remote=True, yes=True)
        assert remote.deleted == [("s3:bucket/backups/postgres/app", ["app_2.dump", "app_3.dump"])]
        data = json.loads((tmp_path / "history.json").read_text())
        (entries,) = (v for k, v in data.items() if not k.startswith("__"))
        assert "postgres/app/app_1.dump" not in entries

    def test_dropping_the_pattern_later_fetches_the_file(self, remote, tmp_path):
        _pull(tmp_path, engines=["postgres"], databases=["app"], exclude_patterns=["app_1.dump"])
        _pull(tmp_path, engines=["postgres"], databases=["app"])
        assert remote.copies[-1][2] == ["app_1.dump"]

    def test_excludes_work_with_the_manifest_fast_path_and_never_use_the_unchanged_shortcut(self, remote, tmp_path):
        remote.manifest = _manifest_of(FakeRemote.REMOTE)
        _pull(tmp_path, exclude_patterns=["*_3.dump"])
        assert remote.listings == 0 and "app_3.dump" not in _fetched(remote)
        before = len(remote.copies)
        _pull(tmp_path)  # without the exclude, the shortcut must not hide app_3.dump
        assert len(remote.copies) == before + 1 and remote.copies[-1][2] == ["app_3.dump"]

    def test_config_patterns_are_merged_with_the_flags(self):
        cfg = Config()
        _load_pull(cfg, lambda key, default="": {"PULL_EXCLUDE": " *.tmp | *_inc.* |*.tmp"}.get(key, default))
        assert cfg.pull_exclude == "*.tmp | *_inc.* |*.tmp"
        assert _exclude_list(cfg, ["x*", "*.tmp"]) == ["x*", "*.tmp", "*_inc.*"]

    def test_config_exclude_is_applied_by_run_pull(self, remote, tmp_path):
        cfg = _cfg(tmp_path)
        cfg.pull_exclude = "*_2.dump|bill_*"
        pull.run_pull(cfg, remote="s3:bucket/backups", engines=["postgres"])
        assert _fetched(remote) == ["app_1.dump", "app_3.dump"]

    def test_the_report_counts_excluded_files(self, remote, tmp_path, monkeypatch):
        docs: list[dict] = []
        monkeypatch.setattr(pull, "json_enabled", lambda: True)
        monkeypatch.setattr(pull, "emit", docs.append)
        _pull(tmp_path, engines=["postgres"], databases=["app"], exclude_patterns=["app_1.dump"])
        assert docs[0]["excluded"] == 1 and docs[0]["jobs"][0]["excluded"] == 1


def test_downloaded_file_lands_where_expected(remote, tmp_path):
    _pull(tmp_path, engines=["postgres"], databases=["app"], exclude_patterns=["app_1.dump"])
    assert not Path(tmp_path / "local" / "postgres" / "app" / "app_1.dump").exists()
    assert Path(tmp_path / "local" / "postgres" / "app" / "app_2.dump").exists()
