"""Tests for pulling backups from an rclone remote."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from fuelrod_backup import pull, pullhistory
from fuelrod_backup.config import Config
from fuelrod_backup.pull import build_args, discover_jobs, run_pull

LISTING = {
    "s3:bucket/backups": "postgres/\nmariadb/\n",
    "s3:bucket/backups/postgres": "app/\nbilling/\n",
    "s3:bucket/backups/mariadb": "shop/\n",
}


def _proc(stdout: str = "", stderr: str = "", code: int = 0):
    return SimpleNamespace(returncode=code, stdout=stdout.encode(), stderr=stderr.encode())




def _cfg(tmp_path) -> Config:
    cfg = Config()
    cfg.base_dir = str(tmp_path / "local")
    return cfg


DAY = 86400


class FakeRemote:
    """A fake rclone: lsf/lsjson serve REMOTE, copy 'downloads' into dest, delete/check are recorded."""

    REMOTE = {
        "s3:bucket/backups/postgres/app": [("app_1.dump", 100, 3), ("app_2.dump", 100, 2), ("app_3.dump", 100, 1)],
        "s3:bucket/backups/postgres/billing": [("bill_1.dump", 50, 2)],
        "s3:bucket/backups/mariadb/shop": [("shop_1.sql", 10, 1)],
    }

    def __init__(self):
        self.copies: list[tuple[str, str, list[str]]] = []
        self.deleted: list[tuple[str, list[str]]] = []
        self.checked: list[list[str]] = []
        self.fail_copy: set[str] = set()
        self.fail_delete = False
        self.manifest: dict | None = None  # what `rclone cat <remote>/manifest.json` returns (None = absent)
        self.uploaded_manifests: list[dict] = []
        self.listings = 0  # lsf / lsjson calls: the traversal the manifest is meant to avoid
        self.active = self.peak = 0
        self.lock = threading.Lock()

    def __call__(self, cmd, **kwargs):
        sub = cmd[1]
        if sub == "cat":
            if self.manifest is None:
                return _proc(stderr="ERROR : object not found", code=3)
            return _proc(json.dumps(self.manifest))
        if sub == "copyto":
            self.uploaded_manifests.append(json.loads(Path(cmd[2]).read_text()))
            return _proc()
        if sub in ("lsf", "lsjson"):
            self.listings += 1
        if sub == "lsf":
            return _proc(LISTING.get(cmd[3], ""))
        if sub == "lsjson":
            items = [
                {"Path": name, "Size": size,
                 "ModTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - age * DAY))}
                for name, size, age in self.REMOTE.get(cmd[5], [])
            ]
            return _proc(json.dumps(items))
        listing = Path(cmd[cmd.index("--files-from-raw") + 1]).read_text().split()
        if sub == "copy":
            with self.lock:
                self.active += 1
                self.peak = max(self.peak, self.active)
            time.sleep(0.03)
            with self.lock:
                self.active -= 1
            if cmd[2] in self.fail_copy:
                return _proc(stderr="ERROR : boom: permission denied", code=1)
            sizes = {name: size for name, size, _a in self.REMOTE[cmd[2]]}
            self.copies.append((cmd[2], cmd[3], listing))
            if "--dry-run" in cmd:
                return _proc()
            for name in listing:
                target = Path(cmd[3]) / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b"x" * sizes[name])
            return _proc()
        if sub == "delete":
            if self.fail_delete:
                return _proc(stderr="ERROR : permission denied", code=1)
            if "--dry-run" in cmd:
                return _proc()
            self.deleted.append((cmd[2], listing))
            return _proc()
        if sub == "check":
            self.checked.append(listing)
            return _proc("".join(f"= {name}\n" for name in listing), code=0)
        raise AssertionError(cmd)


@pytest.fixture
def rclone(monkeypatch, tmp_path):
    fake = FakeRemote()
    monkeypatch.setattr(pull.subprocess, "run", fake)
    monkeypatch.setattr(pull, "rclone_available", lambda: True)
    monkeypatch.setattr(pullhistory, "default_path", lambda: tmp_path / "history.json")
    return fake


class TestDiscover:
    def test_one_job_per_engine_and_database(self, rclone, tmp_path):
        jobs = discover_jobs("s3:bucket/backups", tmp_path, [])
        assert [j.label for j in jobs] == ["mariadb/shop", "postgres/app", "postgres/billing"]
        assert jobs[1].dest == tmp_path / "postgres" / "app"
        assert jobs[1].source == "s3:bucket/backups/postgres/app"

    def test_filters(self, rclone, tmp_path):
        jobs = discover_jobs("s3:bucket/backups", tmp_path, [], engines=["postgres"], databases=["app"])
        assert [j.label for j in jobs] == ["postgres/app"]

    def test_flat_remote_falls_back_to_single_job(self, monkeypatch, tmp_path):
        monkeypatch.setattr(pull.subprocess, "run", lambda cmd, **kw: _proc(""))
        jobs = discover_jobs("s3:bucket/backups", tmp_path, [])
        assert [j.label for j in jobs] == ["(all)"]

    def test_unsafe_remote_names_are_skipped(self, monkeypatch, tmp_path):
        listing = {"s3:b": "postgres/\n..\n.hidden/\n", "s3:b/postgres": "ok/\n../evil/\n"}
        monkeypatch.setattr(pull.subprocess, "run", lambda cmd, **kw: _proc(listing.get(cmd[3], "")))
        jobs = discover_jobs("s3:b", tmp_path, [])
        assert [j.label for j in jobs] == ["postgres/ok"]


















class TestRootSafety:
    def test_refuses_to_pull_an_arbitrary_directory(self, monkeypatch, tmp_path):
        monkeypatch.setattr(pull.subprocess, "run", lambda cmd, **kw: _proc(".cache/\n.ssh/\ngo/\n"))
        with pytest.raises(pull.StorageError, match="db-backup"):
            discover_jobs("akilimo-bkp:", tmp_path, [])

    def test_explicit_engine_filter_overrides_the_guard(self, monkeypatch, tmp_path):
        listing = {"akilimo-bkp:": "go/\n", "akilimo-bkp:go": "tools/\n"}
        monkeypatch.setattr(pull.subprocess, "run", lambda cmd, **kw: _proc(listing.get(cmd[3], "")))
        jobs = discover_jobs("akilimo-bkp:", tmp_path, [], engines=["go"])
        assert [j.label for j in jobs] == ["go/tools"]

    def test_only_known_engines_are_pulled_when_present(self, monkeypatch, tmp_path):
        listing = {"r:b": "postgres/\n.cache/\nnotes/\n", "r:b/postgres": "app/\n"}
        monkeypatch.setattr(pull.subprocess, "run", lambda cmd, **kw: _proc(listing.get(cmd[3], "")))
        assert [j.label for j in discover_jobs("r:b", tmp_path, [])] == ["postgres/app"]


class TestPullInit:
    def _q(self, answers, remote="akilimo-bkp"):
        class Q:
            class Choice:
                def __init__(self, title, value=None):
                    self.title, self.value = title, value

            def _ask(self, value):
                class A:
                    def ask(_self):
                        return value
                return A()

            def select(self, msg, **kw):
                return self._ask(remote)

            def text(self, msg, **kw):
                return self._ask(answers.get(msg.split(" (")[0], ""))

            def confirm(self, msg, **kw):
                return self._ask(False)
        return Q()

    def test_picks_a_configured_remote_and_saves_it(self, tmp_path, monkeypatch):
        import fuelrod_backup.cli as cli_mod

        monkeypatch.setattr("fuelrod_backup.storage.rclone_available", lambda: True)
        monkeypatch.setattr("fuelrod_backup.storage.list_remotes", lambda: ["akilimo-bkp", "other"])
        out = tmp_path / "cfg" / ".backup"
        cli_mod._save_remote_target(self._q({"Path inside the remote": "/db-backup/"}), out, "PULL")
        assert "PULL_REMOTE=akilimo-bkp:db-backup" in out.read_text()

    def test_without_remotes_falls_back_to_typed_spec(self, tmp_path, monkeypatch):
        import fuelrod_backup.cli as cli_mod

        monkeypatch.setattr("fuelrod_backup.storage.rclone_available", lambda: False)
        out = tmp_path / ".backup"
        out.write_text("BASE_DIR=/x\n")
        cli_mod._save_remote_target(self._q({"rclone remote spec": "r:backups"}), out, "PULL")
        text = out.read_text()
        assert "PULL_REMOTE=r:backups" in text and "BASE_DIR=/x" in text

    def test_blank_spec_cancels_without_touching_config(self, tmp_path, monkeypatch):
        import fuelrod_backup.cli as cli_mod

        monkeypatch.setattr("fuelrod_backup.storage.rclone_available", lambda: False)
        out = tmp_path / ".backup"
        cli_mod._save_remote_target(self._q({"rclone remote spec": ""}), out, "PULL")
        assert not out.exists()


class TestArgs:
    def test_throttle_and_parallelism_flags(self):
        args = build_args(Config(), "s3", transfers=2, checkers=3, bwlimit="10M", dry_run=True)
        assert args[args.index("--transfers") + 1] == "2" and args[args.index("--checkers") + 1] == "3"
        assert args[args.index("--bwlimit") + 1] == "10M" and "--dry-run" in args

    def test_which_files_to_fetch_is_not_left_to_rclone(self):
        args = build_args(Config(), "s3", transfers=4, checkers=8, bwlimit=None, dry_run=False)
        assert not {"--ignore-existing", "--include", "--max-age"} & set(args)


def _pull(tmp_path, **kw):
    return run_pull(_cfg(tmp_path), remote="s3:bucket/backups", **kw)


class TestRunPull:
    def test_runs_jobs_in_parallel_and_downloads_everything_new(self, rclone, tmp_path):
        assert _pull(tmp_path, workers=3) == 0
        assert len(rclone.copies) == 3 and rclone.peak > 1
        assert (tmp_path / "local" / "postgres" / "app" / "app_3.dump").stat().st_size == 100

    def test_workers_one_is_sequential(self, rclone, tmp_path):
        _pull(tmp_path, workers=1)
        assert rclone.peak == 1

    def test_failed_job_gives_nonzero_exit_but_others_finish(self, rclone, tmp_path):
        rclone.fail_copy.add("s3:bucket/backups/postgres/app")
        assert _pull(tmp_path) == 1
        assert len(rclone.copies) == 2

    def test_dry_run_creates_nothing_and_records_no_history(self, rclone, tmp_path):
        _pull(tmp_path, dry_run=True)
        assert not (tmp_path / "local").exists()
        assert not (tmp_path / "history.json").exists()

    def test_requires_remote(self, rclone, tmp_path):
        with pytest.raises(SystemExit):
            run_pull(_cfg(tmp_path))

    def test_rejects_bad_parallelism(self, rclone, tmp_path):
        with pytest.raises(SystemExit):
            _pull(tmp_path, workers=0)

    def test_since_and_include_filter_the_fetch_list(self, rclone, tmp_path):
        _pull(tmp_path, since=3, include_patterns=["app_*"], engines=["postgres"], databases=["app"])
        assert [names for _s, _d, names in rclone.copies] == [["app_2.dump", "app_3.dump"]]


class TestNoRedownload:
    def test_second_pull_downloads_nothing(self, rclone, tmp_path):
        _pull(tmp_path)
        first = len(rclone.copies)
        _pull(tmp_path)
        assert len(rclone.copies) == first

    def test_files_already_in_the_folder_are_not_fetched(self, rclone, tmp_path):
        app = tmp_path / "local" / "postgres" / "app"
        app.mkdir(parents=True)
        (app / "app_1.dump").write_bytes(b"x" * 100)
        _pull(tmp_path, engines=["postgres"], databases=["app"])
        assert rclone.copies[0][2] == ["app_2.dump", "app_3.dump"]

    def test_a_local_file_of_a_different_size_is_fetched_again(self, rclone, tmp_path):
        app = tmp_path / "local" / "postgres" / "app"
        app.mkdir(parents=True)
        (app / "app_1.dump").write_bytes(b"x" * 7)  # truncated earlier download
        _pull(tmp_path, engines=["postgres"], databases=["app"])
        assert "app_1.dump" in rclone.copies[0][2]

    def test_deleting_the_local_copy_does_not_cause_a_redownload(self, rclone, tmp_path):
        """The push-then-pull cycle: push removes local files, pull must not fetch them again."""
        _pull(tmp_path)
        before = len(rclone.copies)
        for f in (tmp_path / "local").rglob("*.dump"):
            f.unlink()
        _pull(tmp_path)
        assert len(rclone.copies) == before

    def test_ignore_history_fetches_missing_local_files_again(self, rclone, tmp_path):
        _pull(tmp_path, engines=["postgres"], databases=["app"])
        for f in (tmp_path / "local" / "postgres" / "app").glob("*.dump"):
            f.unlink()
        _pull(tmp_path, engines=["postgres"], databases=["app"], ignore_history=True)
        assert len(rclone.copies) == 2

    def test_overwrite_refetches_everything(self, rclone, tmp_path):
        _pull(tmp_path, engines=["postgres"], databases=["app"])
        _pull(tmp_path, engines=["postgres"], databases=["app"], overwrite=True)
        assert rclone.copies[1][2] == ["app_1.dump", "app_2.dump", "app_3.dump"]

    def test_history_only_counts_a_file_when_the_size_matches(self, rclone, tmp_path, monkeypatch):
        _pull(tmp_path, engines=["postgres"], databases=["app"])
        for f in (tmp_path / "local" / "postgres" / "app").glob("*.dump"):
            f.unlink()
        monkeypatch.setitem(FakeRemote.REMOTE, "s3:bucket/backups/postgres/app", [("app_1.dump", 999, 3)])
        _pull(tmp_path, engines=["postgres"], databases=["app"])
        assert rclone.copies[-1][2] == ["app_1.dump"]  # same name, new size = a different file

    def test_history_forgets_files_that_left_the_remote(self, rclone, tmp_path, monkeypatch):
        _pull(tmp_path, engines=["postgres"], databases=["app"])
        monkeypatch.setitem(FakeRemote.REMOTE, "s3:bucket/backups/postgres/app", [("app_3.dump", 100, 1)])
        _pull(tmp_path, engines=["postgres"], databases=["app"])
        data = json.loads((tmp_path / "history.json").read_text())
        (entries,) = (v for k, v in data.items() if not k.startswith("__"))
        assert sorted(entries) == ["postgres/app/app_3.dump"]


class TestDeleteRemote:
    def test_requires_confirmation(self, rclone, tmp_path):
        with pytest.raises(SystemExit):
            _pull(tmp_path, delete_remote=True)
        assert rclone.copies == [] and rclone.deleted == []

    def test_deletes_only_files_that_are_in_the_local_folder(self, rclone, tmp_path):
        _pull(tmp_path, engines=["postgres"], databases=["app"], delete_remote=True, yes=True)
        assert rclone.deleted == [("s3:bucket/backups/postgres/app", ["app_1.dump", "app_2.dump", "app_3.dump"])]

    def test_a_failed_download_deletes_nothing_on_the_server(self, rclone, tmp_path):
        rclone.fail_copy.add("s3:bucket/backups/postgres/app")
        _pull(tmp_path, engines=["postgres"], databases=["app"], delete_remote=True, yes=True)
        assert rclone.deleted == []

    def test_files_missing_locally_are_never_deleted_remotely(self, rclone, tmp_path, monkeypatch):
        # the download "succeeds" but nothing lands on disk
        original = rclone.__call__

        def no_files(cmd, **kw):
            if cmd[1] == "copy":
                return _proc()
            return original(cmd, **kw)

        monkeypatch.setattr(pull.subprocess, "run", no_files)
        _pull(tmp_path, engines=["postgres"], databases=["app"], delete_remote=True, yes=True)
        assert rclone.deleted == []

    def test_keep_remote_leaves_the_newest_backups_on_the_server(self, rclone, tmp_path):
        _pull(tmp_path, engines=["postgres"], databases=["app"], delete_remote=True, keep_remote=1, yes=True)
        assert rclone.deleted == [("s3:bucket/backups/postgres/app", ["app_1.dump", "app_2.dump"])]

    def test_dry_run_deletes_nothing_even_without_yes(self, rclone, tmp_path):
        assert _pull(tmp_path, delete_remote=True, dry_run=True) == 0
        assert rclone.deleted == []

    def test_verify_runs_rclone_check_and_keeps_only_confirmed_files(self, rclone, tmp_path, monkeypatch):
        original = rclone.__call__

        def check_confirms_one(cmd, **kw):
            if cmd[1] == "check":
                return _proc("= app_2.dump\n* app_1.dump\n")
            return original(cmd, **kw)

        monkeypatch.setattr(pull.subprocess, "run", check_confirms_one)
        _pull(tmp_path, engines=["postgres"], databases=["app"], delete_remote=True, verify=True, yes=True)
        assert rclone.deleted == [("s3:bucket/backups/postgres/app", ["app_2.dump"])]

    def test_delete_failure_marks_the_job_failed(self, rclone, tmp_path):
        rclone.fail_delete = True
        assert _pull(tmp_path, engines=["postgres"], databases=["app"], delete_remote=True, yes=True) == 1

    def test_config_defaults_are_used(self, rclone, tmp_path):
        cfg = _cfg(tmp_path)
        cfg.pull_delete_remote, cfg.pull_keep_remote = True, 2
        run_pull(cfg, remote="s3:bucket/backups", engines=["postgres"], databases=["app"], yes=True)
        assert rclone.deleted == [("s3:bucket/backups/postgres/app", ["app_1.dump"])]


def test_pull_delete_settings_load_from_config():
    from fuelrod_backup.config import _load_pull

    cfg = Config()
    values = {"PULL_DELETE_REMOTE": "true", "PULL_KEEP_REMOTE": "3"}
    _load_pull(cfg, lambda key, default="": values.get(key, default))
    assert cfg.pull_delete_remote is True and cfg.pull_keep_remote == 3
    _load_pull(cfg, lambda key, default="": {"PULL_KEEP_REMOTE": "x"}.get(key, default))
    assert cfg.pull_delete_remote is False and cfg.pull_keep_remote == 0
