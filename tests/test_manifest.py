"""Tests for the global manifest.json at the root of the backup folder."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from fuelrod_backup import pull, pullhistory, storage
from fuelrod_backup.config import Config
from fuelrod_backup.manifest import MANIFEST_NAME, build, fingerprint, parse, read_local, scan_local, write_local
from fuelrod_backup.pull import jobs_from_manifest
from fuelrod_backup.quota import RemoteFile
from tests.conftest import patch_rclone
from tests.test_pull import FakeRemote, _cfg, _pull  # noqa: F401 — shared fake rclone fixture helpers

NOW = time.time()


def rf(path, size=10, age=1.0):
    return RemoteFile(path=path, size=size, mtime=NOW - age * 86400)


def _tree(tmp_path) -> Path:
    base = tmp_path / "backups"
    for engine, db, name in [("postgres", "app", "app_1.dump"), ("postgres", "bill", "bill_1.dump"),
                             ("mariadb", "shop", "shop_1.sql")]:
        d = base / engine / db
        d.mkdir(parents=True, exist_ok=True)
        (d / name).write_bytes(b"x" * 7)
    return base


class TestDocument:
    def test_global_manifest_lists_every_engine_and_database(self, tmp_path):
        base = _tree(tmp_path)
        path = write_local(base)
        assert path == base / MANIFEST_NAME
        data = json.loads(path.read_text())
        assert data["version"] == 1
        assert [f["path"] for f in data["files"]] == [
            "mariadb/shop/shop_1.sql", "postgres/app/app_1.dump", "postgres/bill/bill_1.dump",
        ]
        assert all(f["size"] == 7 for f in data["files"])

    def test_hidden_files_and_the_manifest_itself_are_not_listed(self, tmp_path):
        base = _tree(tmp_path)
        (base / ".fuelrod-postgres.lock").write_text("x")
        (base / "postgres" / "app" / ".incremental_state.json").write_text("{}")
        write_local(base)
        write_local(base)  # a second run must not list the first run's manifest
        assert all(".lock" not in f.path and ".incremental" not in f.path and f.path != MANIFEST_NAME
                   for f in scan_local(base))
        assert len(read_local(base)) == 3

    def test_roundtrip_and_fingerprint_is_stable(self, tmp_path):
        base = _tree(tmp_path)
        write_local(base)
        first = json.loads((base / MANIFEST_NAME).read_text())["fingerprint"]
        write_local(base)
        assert json.loads((base / MANIFEST_NAME).read_text())["fingerprint"] == first

    def test_fingerprint_changes_when_a_file_changes(self, tmp_path):
        base = _tree(tmp_path)
        write_local(base)
        before = json.loads((base / MANIFEST_NAME).read_text())["fingerprint"]
        (base / "postgres" / "app" / "app_2.dump").write_bytes(b"y")
        write_local(base)
        assert json.loads((base / MANIFEST_NAME).read_text())["fingerprint"] != before

    def test_tampered_or_wrong_version_manifests_are_rejected(self):
        doc = build([rf("postgres/app/a.dump")])
        assert parse(doc) is not None
        assert parse({**doc, "files": []}) is None  # fingerprint no longer matches
        assert parse({**doc, "version": 99}) is None
        assert parse("not a dict") is None
        assert parse({"version": 1, "files": [{"path": "x"}]}) is None

    def test_fingerprint_ignores_sub_second_mtime_differences(self):
        a = [RemoteFile("p/a/x.dump", 5, 1000.9)]
        b = [RemoteFile("p/a/x.dump", 5, 1000.1)]
        assert fingerprint(a) == fingerprint(b)


class TestJobsFromManifest:
    FILES = [rf("postgres/app/a1.dump"), rf("postgres/app/a2.dump"), rf("postgres/bill/b1.dump"),
             rf("mariadb/shop/s1.sql"), rf("notes/x/readme.txt")]

    def test_one_job_per_engine_database_with_relative_paths_and_no_listing(self, tmp_path):
        jobs = jobs_from_manifest(self.FILES, "r:b", tmp_path)
        assert [j.label for j in jobs] == ["mariadb/shop", "postgres/app", "postgres/bill"]  # 'notes' is not an engine
        app = next(j for j in jobs if j.label == "postgres/app")
        assert [f.path for f in app.files] == ["a1.dump", "a2.dump"] and app.source == "r:b/postgres/app"

    def test_filters(self, tmp_path):
        jobs = jobs_from_manifest(self.FILES, "r:b", tmp_path, engines=["postgres"], databases=["bill"])
        assert [j.label for j in jobs] == ["postgres/bill"]

    def test_unexpected_layout_falls_back_to_listing(self, tmp_path):
        assert jobs_from_manifest([rf("flat.dump")], "r:b", tmp_path) is None
        assert jobs_from_manifest([rf("notes/x/readme.txt")], "r:b", tmp_path) is None


def _manifest_of(remote: dict) -> dict:
    files = []
    for source, rows in remote.items():
        prefix = source.split("backups/", 1)[1]
        files += [rf(f"{prefix}/{name}", size, age) for name, size, age in rows]
    return build(files)


@pytest.fixture
def remote(monkeypatch, tmp_path):
    fake = FakeRemote()
    monkeypatch.setattr(pull.subprocess, "run", fake)
    monkeypatch.setattr(pull, "rclone_available", lambda: True)
    monkeypatch.setattr(pullhistory, "default_path", lambda: tmp_path / "history.json")
    fake.manifest = _manifest_of(FakeRemote.REMOTE)
    return fake


class TestPullUsesTheManifest:
    def test_no_folder_is_listed_when_a_manifest_exists(self, remote, tmp_path):
        assert _pull(tmp_path) == 0
        assert remote.listings == 0
        assert len(remote.copies) == 3
        assert (tmp_path / "local" / "postgres" / "app" / "app_3.dump").exists()

    def test_unchanged_manifest_means_nothing_at_all_is_contacted(self, remote, tmp_path):
        _pull(tmp_path)
        calls_before = len(remote.copies)
        assert _pull(tmp_path) == 0
        assert remote.listings == 0 and len(remote.copies) == calls_before

    def test_a_new_backup_changes_the_fingerprint_and_is_fetched(self, remote, tmp_path):
        _pull(tmp_path)
        remote.REMOTE = {**FakeRemote.REMOTE}
        remote.REMOTE["s3:bucket/backups/postgres/app"] = [*FakeRemote.REMOTE["s3:bucket/backups/postgres/app"],
                                                           ("app_4.dump", 100, 0.1)]
        try:
            remote.manifest = _manifest_of(remote.REMOTE)
            _pull(tmp_path)
        finally:
            remote.REMOTE = FakeRemote.REMOTE
        assert remote.copies[-1][2] == ["app_4.dump"]

    def test_no_manifest_flag_forces_listing(self, remote, tmp_path):
        _pull(tmp_path, use_manifest=False)
        assert remote.listings > 0

    def test_missing_manifest_falls_back_to_listing(self, remote, tmp_path):
        remote.manifest = None
        assert _pull(tmp_path) == 0
        assert remote.listings > 0 and len(remote.copies) == 3

    def test_filtered_pulls_never_record_the_fingerprint(self, remote, tmp_path):
        _pull(tmp_path, engines=["postgres"], databases=["app"])
        data = json.loads((tmp_path / "history.json").read_text())
        assert data.get("__manifest_fingerprints__", {}) == {}
        _pull(tmp_path)  # the later full pull must still look at everything
        assert len(remote.copies) == 3

    def test_a_failed_job_does_not_record_the_fingerprint(self, remote, tmp_path):
        remote.fail_copy.add("s3:bucket/backups/postgres/app")
        _pull(tmp_path)
        remote.fail_copy.clear()
        _pull(tmp_path)
        assert (tmp_path / "local" / "postgres" / "app" / "app_1.dump").exists()

    def test_overwrite_and_ignore_history_skip_the_shortcut(self, remote, tmp_path):
        _pull(tmp_path)
        n = len(remote.copies)
        _pull(tmp_path, ignore_history=True)
        assert len(remote.copies) == n  # history skipped, but files are still on disk

    def test_stale_manifest_falls_back_to_a_listing_for_that_job(self, remote, tmp_path):
        """The manifest lists a file the server no longer has: the download fails, a real listing fixes it."""
        stale = _manifest_of({**FakeRemote.REMOTE,
                              "s3:bucket/backups/postgres/app": [*FakeRemote.REMOTE["s3:bucket/backups/postgres/app"],
                                                                 ("ghost.dump", 100, 0.5)]})
        remote.manifest = stale
        original = remote.__call__

        def strict(cmd, **kw):
            if cmd[1] == "copy":
                names = Path(cmd[cmd.index("--files-from-raw") + 1]).read_text().split()
                if "ghost.dump" in names:
                    from tests.test_pull import _proc

                    return _proc(stderr="ERROR : ghost.dump: couldn't find file", code=3)
            return original(cmd, **kw)

        import fuelrod_backup.pull as pull_mod

        pull_mod.subprocess.run = strict
        try:
            assert _pull(tmp_path, engines=["postgres"], databases=["app"]) == 0
        finally:
            pull_mod.subprocess.run = remote
        assert remote.listings >= 1  # the retry listed the folder
        assert (tmp_path / "local" / "postgres" / "app" / "app_3.dump").exists()

    def test_deleting_on_the_server_publishes_a_fresh_manifest(self, remote, tmp_path):
        _pull(tmp_path, engines=["postgres"], databases=["app"], delete_remote=True, yes=True)
        assert remote.uploaded_manifests  # refreshed after the deletions


class TestPushPublishesTheManifest:
    def _setup(self, tmp_path, monkeypatch):
        base = _tree(tmp_path)
        cfg = Config()
        cfg.base_dir = str(base)
        uploads = []
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage, "_upload", lambda *a, **k: None)

        def run(args, timeout=0):
            uploads.append(args)
            if args[0] == "lsjson":
                items = [{"Path": "postgres/app/app_1.dump", "Size": 7, "ModTime": "2026-01-01T00:00:00Z"}]
                return 0, json.dumps(items), ""
            return 0, "", ""

        patch_rclone(monkeypatch, run)
        patch_rclone(monkeypatch, run)

        patch_rclone(monkeypatch, run)
        return cfg, uploads

    def test_destination_manifest_is_written_after_a_push(self, tmp_path, monkeypatch):
        cfg, calls = self._setup(tmp_path, monkeypatch)
        storage.run_sync(cfg, remote="s3:b/x", interactive=False, delete_local=False)
        assert any(c[0] == "copyto" and c[2].endswith("/manifest.json") for c in calls)

    def test_dry_run_and_the_manifest_switch_publish_nothing(self, tmp_path, monkeypatch):
        cfg, calls = self._setup(tmp_path, monkeypatch)
        storage.run_sync(cfg, remote="s3:b/x", interactive=False, delete_local=False, dry_run=True)
        cfg.manifest = False
        storage.run_sync(cfg, remote="s3:b/x", interactive=False, delete_local=False)
        assert not any(c[0] == "copyto" for c in calls)

    def test_the_local_manifest_is_refreshed_after_local_files_are_deleted(self, tmp_path, monkeypatch):
        cfg, _calls = self._setup(tmp_path, monkeypatch)
        write_local(Path(cfg.base_dir))
        before = len(read_local(Path(cfg.base_dir)))
        storage.run_sync(cfg, remote="s3:b/x", interactive=False, delete_local=True)
        assert len(read_local(Path(cfg.base_dir))) < before

    def test_the_local_manifest_is_never_pushed_as_a_backup_file(self, tmp_path):
        base = _tree(tmp_path)
        write_local(base)
        assert all(f.name != MANIFEST_NAME for f in storage._collect_files(base, ["*"]))


class TestBackupWritesTheManifest:
    def test_write_manifest_helper_honours_the_config_switch(self, tmp_path):
        from fuelrod_backup.backup import _write_manifest

        base = _tree(tmp_path)
        cfg = Config()
        cfg.base_dir = str(base)
        _write_manifest(cfg)
        assert (base / MANIFEST_NAME).exists()
        (base / MANIFEST_NAME).unlink()
        cfg.manifest = False
        _write_manifest(cfg)
        assert not (base / MANIFEST_NAME).exists()

    def test_config_key(self):
        from fuelrod_backup.config import _load_ops

        cfg = Config()
        _load_ops(cfg, lambda key, default="": {"MANIFEST": "false"}.get(key, default))
        assert cfg.manifest is False
        _load_ops(cfg, lambda key, default="": default)
        assert cfg.manifest is True

    def test_retention_cleanup_then_manifest_reflects_what_is_left(self, tmp_path):
        base = _tree(tmp_path)
        old = base / "postgres" / "app" / "app_1.dump"
        os.utime(old, (1, 1))
        from fuelrod_backup.backup import _cleanup_old, _write_manifest

        cfg = Config()
        cfg.base_dir = str(base)
        _cleanup_old(str(base), 1)
        _write_manifest(cfg)
        assert "postgres/app/app_1.dump" not in [f.path for f in read_local(base)]
