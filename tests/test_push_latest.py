"""Tests for uploading only the newest N backups per database (push --latest)."""

from __future__ import annotations

import os
import time
from pathlib import Path

from fuelrod_backup import storage
from fuelrod_backup.config import Config, _load_pull
from fuelrod_backup.storage import run_sync, select_latest


def _make(db_dir: Path, name: str, age_days: float) -> Path:
    db_dir.mkdir(parents=True, exist_ok=True)
    f = db_dir / name
    f.write_text("x")
    ts = time.time() - age_days * 86400
    os.utime(f, (ts, ts))
    return f


def _names(files):
    return sorted(f.name for f in files)


class TestSelectLatest:
    def test_keeps_newest_n_per_database(self, tmp_path):
        app = tmp_path / "postgres" / "app"
        bill = tmp_path / "postgres" / "billing"
        files = [_make(app, f"app_{i}.dump", age_days=i) for i in range(1, 6)]
        files += [_make(bill, f"billing_{i}.dump", age_days=i) for i in range(1, 3)]
        kept = select_latest(files, 3)
        assert _names(kept) == ["app_1.dump", "app_2.dump", "app_3.dump", "billing_1.dump", "billing_2.dump"]

    def test_manifest_travels_with_its_backup_and_others_are_skipped(self, tmp_path):
        db = tmp_path / "postgres" / "app"
        files = [
            _make(db, "app_20260101_000000.dump", 3),
            _make(db, "app_20260102_000000.dump", 2),
            _make(db, "app_20260103_000000.dump", 1),
            _make(db, "manifest_20260101_000000.txt", 3),
            _make(db, "manifest_20260103_000000.txt", 1),
            _make(db, ".incremental_state.json", 1),
        ]
        kept = select_latest(files, 2)
        assert _names(kept) == [
            "app_20260102_000000.dump", "app_20260103_000000.dump", "manifest_20260103_000000.txt",
        ]

    def test_selected_increment_pulls_in_its_full_base(self, tmp_path):
        db = tmp_path / "postgres" / "app"
        files = [
            _make(db, "app_full_old.dump", 10),
            _make(db, "app_full_base.dump", 5),
            _make(db, "app_a_inc.dump", 3),
            _make(db, "app_b_inc.dump", 2),
            _make(db, "app_c_inc.dump", 1),
        ]
        kept = select_latest(files, 2)
        assert _names(kept) == ["app_b_inc.dump", "app_c_inc.dump", "app_full_base.dump"]

    def test_encrypted_and_compressed_artifacts_count(self, tmp_path):
        db = tmp_path / "mariadb" / "shop"
        files = [_make(db, f"shop_{i}.sql.gz.age", i) for i in range(1, 5)]
        assert len(select_latest(files, 3)) == 3

    def test_zero_keeps_everything(self, tmp_path):
        db = tmp_path / "postgres" / "app"
        files = [_make(db, f"app_{i}.dump", i) for i in range(1, 6)]
        assert select_latest(files, 0) == files


class TestPushLatestUpload:
    def _setup(self, tmp_path, monkeypatch, **cfg_kw):
        base = tmp_path / "backups"
        for i in range(1, 6):
            _make(base / "postgres" / "app", f"app_{i}.dump", i)
        cfg = Config()
        cfg.base_dir = str(base)
        for key, value in cfg_kw.items():
            setattr(cfg, key, value)
        seen = {}

        def fake_upload(base_dir, remote, args, count):
            seen["args"] = list(args)
            seen["count"] = count
            seen["listed"] = None
            if "--files-from" in args:
                seen["listed"] = Path(args[args.index("--files-from") + 1]).read_text().split()

        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage, "_upload", fake_upload)
        monkeypatch.setattr(storage, "_run_rclone", lambda args, timeout=0: (0, "", ""))
        return cfg, seen

    def test_default_uploads_only_the_latest_three(self, tmp_path, monkeypatch):
        cfg, seen = self._setup(tmp_path, monkeypatch)
        run_sync(cfg, interactive=False, remote="s3:b/x", delete_local=False)
        assert seen["count"] == 3
        assert sorted(seen["listed"]) == [f"postgres/app/app_{i}.dump" for i in (1, 2, 3)]
        assert "--include" not in seen["args"]

    def test_latest_flag_overrides_config_and_zero_means_all(self, tmp_path, monkeypatch):
        cfg, seen = self._setup(tmp_path, monkeypatch)
        run_sync(cfg, interactive=False, remote="s3:b/x", delete_local=False, latest=2)
        assert seen["count"] == 2
        run_sync(cfg, interactive=False, remote="s3:b/x", delete_local=False, latest=0)
        assert seen["count"] == 5 and seen["listed"] is None and "--include" in seen["args"]

    def test_only_uploaded_files_are_deleted_locally(self, tmp_path, monkeypatch):
        cfg, _seen = self._setup(tmp_path, monkeypatch)
        run_sync(cfg, interactive=False, remote="s3:b/x", delete_local=True)
        left = sorted(p.name for p in (tmp_path / "backups" / "postgres" / "app").iterdir())
        assert left == ["app_4.dump", "app_5.dump"]

    def test_temp_file_list_is_cleaned_up(self, tmp_path, monkeypatch):
        cfg, seen = self._setup(tmp_path, monkeypatch)
        run_sync(cfg, interactive=False, remote="s3:b/x", delete_local=False)
        listing = Path(seen["args"][seen["args"].index("--files-from") + 1])
        assert not listing.exists()

    def test_config_value_is_loaded(self):
        cfg = Config()
        _load_pull(cfg, lambda key, default="": {"PUSH_LATEST": "5"}.get(key, default))
        assert cfg.push_latest == 5
        _load_pull(cfg, lambda key, default="": {"PUSH_LATEST": "junk"}.get(key, default))
        assert cfg.push_latest == 3


class TestDeleteLocalDefault:
    def test_config_value_is_loaded(self):
        cfg = Config()
        _load_pull(cfg, lambda key, default="": {"PUSH_DELETE_LOCAL": "false"}.get(key, default))
        assert cfg.push_delete_local is False
        _load_pull(cfg, lambda key, default="": {}.get(key, default))
        assert cfg.push_delete_local is True

    def test_keep_local_flag_and_config_decide(self, tmp_path, monkeypatch):
        import fuelrod_backup.cli as cli_mod

        seen = {}
        monkeypatch.setattr("fuelrod_backup.storage.run_sync", lambda cfg, **kw: seen.update(kw) or 0)
        cfg = Config()
        monkeypatch.setattr(cli_mod, "load_config", lambda *a, **k: cfg)

        def run(keep):
            seen.clear()
            cli_mod.sync_cmd(
                remote=[], dry_run=False, days=None, include=[], keep_local=keep, list_remotes=False,
                no_interactive=True, transfers=None, checkers=None, since=None, until=None, schema=[],
                latest=None, quota=None, rclone=None, as_json=False, config_file=None,
            )
            return seen["delete_local"]

        assert run(None) is True            # config default
        cfg.push_delete_local = False
        assert run(None) is False           # config says keep
        assert run(False) is True           # --delete-local wins
        assert run(True) is False           # --keep-local wins
