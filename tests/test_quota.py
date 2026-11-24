"""Tests for remote quota monitoring and pruning."""

from __future__ import annotations

import json
import time

import pytest

from fuelrod_backup import quota as quota_mod
from fuelrod_backup import storage
from fuelrod_backup.config import Config, _load_pull
from fuelrod_backup.quota import RemoteFile, enforce_quota, parse_size, plan_prune
from tests.conftest import patch_rclone

DAY = 86400
NOW = time.time()


def rf(path: str, size: int, age_days: float) -> RemoteFile:
    return RemoteFile(path=path, size=size, mtime=NOW - age_days * DAY)


class TestParseSize:
    @pytest.mark.parametrize("text,expected", [
        ("1024", 1024), ("500M", 500 * 1024**2), ("50G", 50 * 1024**3), ("1.5T", int(1.5 * 1024**4)),
        ("2 GB", 2 * 1024**3), ("2gib", 2 * 1024**3), (" 10k ", 10 * 1024),
    ])
    def test_valid(self, text, expected):
        assert parse_size(text) == expected

    @pytest.mark.parametrize("text", ["", "abc", "-5G", "5X", "G5"])
    def test_invalid(self, text):
        with pytest.raises(ValueError):
            parse_size(text)


class TestPlanPrune:
    def _files(self):
        # two databases, three backups each (100 bytes), newest = age 1
        return [rf(f"postgres/{db}/{db}_{i}.dump", 100, i) for db in ("app", "bill") for i in (1, 2, 3)]

    def test_nothing_is_deleted_within_quota(self):
        doomed, _protected, _target = plan_prune(self._files(), quota=600, target_pct=90, keep=1)
        assert doomed == []

    def test_deletes_oldest_first_until_target(self):
        doomed, _protected, target = plan_prune(self._files(), quota=500, target_pct=80, keep=1)
        assert target == 400
        assert [f.path for f in doomed] == ["postgres/app/app_3.dump", "postgres/bill/bill_3.dump"] or \
            sorted(f.path for f in doomed) == ["postgres/app/app_3.dump", "postgres/bill/bill_3.dump"]
        assert sum(f.size for f in doomed) == 200

    def test_newest_backup_of_each_database_is_never_deleted(self):
        doomed, protected, _t = plan_prune(self._files(), quota=100, target_pct=50, keep=1)
        names = {f.path for f in doomed}
        assert "postgres/app/app_1.dump" not in names and "postgres/bill/bill_1.dump" not in names
        assert protected == 2 and len(doomed) == 4

    def test_keep_more_protects_more(self):
        doomed, protected, _t = plan_prune(self._files(), quota=100, target_pct=50, keep=2)
        assert protected == 4 and len(doomed) == 2

    def test_manifest_of_protected_backup_is_protected(self):
        files = [
            rf("postgres/app/app_20260101_000000.dump", 100, 2),
            rf("postgres/app/manifest_20260101_000000.txt", 1, 2),
            rf("postgres/app/app_20260102_000000.dump", 100, 1),
            rf("postgres/app/manifest_20260102_000000.txt", 1, 1),
        ]
        doomed, _p, _t = plan_prune(files, quota=100, target_pct=100, keep=1)
        assert {f.path for f in doomed} == {
            "postgres/app/app_20260101_000000.dump", "postgres/app/manifest_20260101_000000.txt",
        }


class FakeRclone:
    """Serves lsjson/about/delete for a fixed file set."""

    def __init__(self, files, about=None, fail_delete=False):
        self.files, self.about, self.fail_delete = list(files), about, fail_delete
        self.deleted: list[str] = []

    def __call__(self, args, timeout=0):
        if args[0] == "lsjson":
            items = [
                {"Path": f.path, "Size": f.size,
                 "ModTime": time.strftime("%Y-%m-%dT%H:%M:%S.123456789Z", time.gmtime(f.mtime))}
                for f in self.files
            ]
            return 0, json.dumps(items), ""
        if args[0] == "about":
            return (0, json.dumps(self.about), "") if self.about else (1, "", "not supported")
        if args[0] == "cat":  # the destination manifest: there is none
            return 1, "", "object not found"
        if args[0] == "copyto":
            return 0, "", ""
        if args[0] == "delete":
            if self.fail_delete:
                return 1, "", "ERROR : permission denied"
            listing = args[args.index("--files-from-raw") + 1]
            with open(listing, encoding="utf-8") as fh:
                self.deleted = fh.read().split()
            return 0, "", ""
        raise AssertionError(args)


@pytest.fixture
def files():
    return [rf(f"postgres/app/app_{i}.dump", 100, i) for i in (1, 2, 3, 4)]


class TestEnforce:
    def test_report_only_does_not_delete(self, files, monkeypatch):
        fake = FakeRclone(files)
        patch_rclone(monkeypatch, fake)
        rep = enforce_quota("r:b", [], quota=300, prune=False)
        assert rep.usage == 400 and rep.over and rep.percent > 100 and fake.deleted == []

    def test_prune_deletes_oldest_and_gets_under_quota(self, files, monkeypatch):
        fake = FakeRclone(files)
        patch_rclone(monkeypatch, fake)
        rep = enforce_quota("r:b", [], quota=300, target_pct=70, keep=1)
        assert fake.deleted == ["postgres/app/app_4.dump", "postgres/app/app_3.dump"]
        assert rep.after == 200 and rep.ok and rep.freed == 200

    def test_dry_run_deletes_nothing_but_reports(self, files, monkeypatch):
        fake = FakeRclone(files)
        patch_rclone(monkeypatch, fake)
        rep = enforce_quota("r:b", [], quota=300, target_pct=70, keep=1, dry_run=True)
        assert fake.deleted == [] and len(rep.deleted) == 2

    def test_too_small_quota_reports_still_over_instead_of_deleting_everything(self, files, monkeypatch):
        fake = FakeRclone(files)
        patch_rclone(monkeypatch, fake)
        rep = enforce_quota("r:b", [], quota=50, keep=1)
        assert "postgres/app/app_1.dump" not in fake.deleted
        assert rep.over and rep.protected == 1

    def test_provider_about_is_included_when_supported(self, files, monkeypatch):
        patch_rclone(monkeypatch, FakeRclone(files, about={"total": 10, "used": 4, "free": 6}))
        assert enforce_quota("r:b", [], quota=10_000, prune=False).provider == {"total": 10, "used": 4, "free": 6}

    def test_listing_failure_raises(self, monkeypatch):
        patch_rclone(monkeypatch, lambda args, timeout=0: (1, "", "ERROR : no such remote"))
        with pytest.raises(storage.StorageError, match="no such remote"):
            enforce_quota("nope:", [], quota=100)

    def test_failed_delete_raises(self, files, monkeypatch):
        patch_rclone(monkeypatch, FakeRclone(files, fail_delete=True))
        with pytest.raises(storage.StorageError, match="permission denied"):
            enforce_quota("r:b", [], quota=300, target_pct=70)


class TestConfigAndSync:
    def test_config_values(self):
        cfg = Config()
        values = {"PUSH_QUOTA": " 50G ", "PUSH_QUOTA_TARGET": "80", "PUSH_QUOTA_KEEP": "2"}
        _load_pull(cfg, lambda key, default="": values.get(key, default))
        assert (cfg.push_quota, cfg.push_quota_target, cfg.push_quota_keep) == ("50G", 80, 2)
        _load_pull(cfg, lambda key, default="": {"PUSH_QUOTA_TARGET": "500", "PUSH_QUOTA_KEEP": "x"}.get(key, default))
        assert (cfg.push_quota_target, cfg.push_quota_keep) == (100, 1)

    def _sync(self, tmp_path, monkeypatch, files, **kw):
        base = tmp_path / "b"
        (base / "postgres" / "app").mkdir(parents=True)
        (base / "postgres" / "app" / "app_1.dump").write_text("x")
        cfg = Config()
        cfg.base_dir = str(base)
        fake = FakeRclone(files)
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage, "_upload", lambda *a, **k: None)
        monkeypatch.setattr(quota_mod, "_run_rclone", fake)
        monkeypatch.setattr("fuelrod_backup.manifest._run_rclone", fake)
        calls = []
        real = None
        monkeypatch.setattr(storage, "_run_rclone", lambda args, timeout=0: calls.append(args) or (0, "", ""))
        code = storage.run_sync(cfg, interactive=False, delete_local=False, remote="r:b", **kw)
        return code, fake, calls, real

    def test_quota_prunes_after_upload_and_skips_the_age_prune(self, tmp_path, monkeypatch, files):
        code, fake, calls, _ = self._sync(tmp_path, monkeypatch, files, quota="300")
        assert code == 0 and fake.deleted  # quota pruning ran
        assert all(args[0] != "delete" for args in calls)  # no 2-day age prune

    def test_explicit_days_still_runs_the_age_prune_with_a_quota(self, tmp_path, monkeypatch, files):
        _code, _fake, calls, _ = self._sync(tmp_path, monkeypatch, files, quota="300", age_days=30)
        assert any(args[0] == "delete" for args in calls)

    def test_nothing_is_pruned_by_default(self, tmp_path, monkeypatch, files):
        """Without --days or a quota, freshly uploaded backups are never deleted from the destination."""
        _code, fake, calls, _ = self._sync(tmp_path, monkeypatch, files)
        assert all(args[0] != "delete" for args in calls) and fake.deleted == []

    def test_invalid_quota_is_rejected(self, tmp_path, monkeypatch, files):
        with pytest.raises(SystemExit):
            self._sync(tmp_path, monkeypatch, files, quota="lots")


class TestBackupUnits:
    def test_dump_and_manifest_are_deleted_together(self):
        files = [
            rf("postgres/app/app_20260101_000000.dump", 100, 4),
            rf("postgres/app/manifest_20260101_000000.txt", 1, 4),
            rf("postgres/app/app_20260102_000000.dump", 100, 3),
            rf("postgres/app/manifest_20260102_000000.txt", 1, 3),
            rf("postgres/app/app_20260103_000000.dump", 100, 2),
            rf("postgres/app/manifest_20260103_000000.txt", 1, 2),
            rf("postgres/app/app_20260104_000000.dump", 100, 1),
            rf("postgres/app/manifest_20260104_000000.txt", 1, 1),
        ]
        # needs 150 bytes freed: the second unit is only partly needed, but must go whole
        doomed, _p, _t = plan_prune(files, quota=250, target_pct=100, keep=1)
        paths = {f.path for f in doomed}
        for stamp in ("20260101_000000", "20260102_000000"):
            assert (f"postgres/app/app_{stamp}.dump" in paths) == (f"postgres/app/manifest_{stamp}.txt" in paths)
        assert "postgres/app/app_20260104_000000.dump" not in paths

    def test_report_only_run_says_over_quota_not_still_over(self, files, monkeypatch, capsys):
        patch_rclone(monkeypatch, FakeRclone(files))
        quota_mod.console.file = __import__("sys").stdout
        rep = enforce_quota("r:b", [], quota=300, prune=False)
        quota_mod.print_report(rep)
        out = capsys.readouterr().out
        assert "Over quota by" in out and "Still over" not in out
