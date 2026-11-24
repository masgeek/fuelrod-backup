"""Tests for pushing to several destinations, including local folders (e.g. a OneDrive folder)."""

from __future__ import annotations

from pathlib import Path

import pytest

from fuelrod_backup import storage
from fuelrod_backup.config import Config
from fuelrod_backup.storage import parse_remote, resolve_destinations, run_sync


class TestLocalDestinations:
    @pytest.mark.parametrize(
        "spec", ["D:/OneDrive/Backups", "C:\\Users\\me\\OneDrive", "/mnt/onedrive/b", "./out", "backups"],
    )
    def test_folders_are_local(self, spec):
        r = parse_remote(spec)
        assert r.provider == "local" and r.path == spec

    def test_rclone_remotes_are_still_remotes(self):
        assert parse_remote("akilimo-bkp:db").provider == "akilimo-bkp"
        assert parse_remote("s3:bucket/x").provider == "s3"

    def test_location_is_a_plain_path_for_local_and_provider_path_for_remotes(self, tmp_path):
        assert parse_remote(str(tmp_path)).location == str(tmp_path)
        assert parse_remote("s3:bucket/x").location == "s3:bucket/x"
        assert parse_remote("s3:").location == "s3:"

    def test_tilde_is_expanded(self):
        assert "~" not in parse_remote("~/OneDrive/Backups").location


class TestResolveDestinations:
    def _cfg(self, **kw):
        cfg = Config()
        for k, v in kw.items():
            setattr(cfg, k, v)
        return cfg

    def test_pipe_separates_destinations_in_config(self):
        cfg = self._cfg(push_remote="akilimo-bkp:backups|D:/OneDrive/Backups")
        assert resolve_destinations(cfg) == ["akilimo-bkp:backups", "D:/OneDrive/Backups"]

    def test_repeated_flags_win_over_config_and_are_deduplicated(self):
        cfg = self._cfg(push_remote="a:x")
        assert resolve_destinations(cfg, ["b:y", "c:z|b:y"]) == ["b:y", "c:z"]

    def test_falls_back_to_the_default_remote(self):
        cfg = self._cfg(remote="s3:fallback")
        assert resolve_destinations(cfg) == ["s3:fallback"]

    def test_blank_explicit_value_is_kept_so_it_errors_instead_of_using_the_default(self):
        cfg = self._cfg(remote="s3:fallback")
        assert resolve_destinations(cfg, "   ") == ["   "]


def _tree(tmp_path):
    base = tmp_path / "backups"
    (base / "postgres" / "app").mkdir(parents=True)
    for i in (1, 2):
        (base / "postgres" / "app" / f"app_{i}.dump").write_text("x")
    return base


@pytest.fixture
def uploads(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(storage, "rclone_available", lambda: True)
    monkeypatch.setattr(storage, "_run_rclone", lambda args, timeout=0: (0, "", ""))

    def fake_upload(base_dir, remote, args, count):
        if "FAIL" in remote:
            raise storage.StorageError("boom")
        calls.append(remote)

    monkeypatch.setattr(storage, "_upload", fake_upload)
    return calls


class TestPushToSeveral:
    def _cfg(self, tmp_path):
        cfg = Config()
        cfg.base_dir = str(_tree(tmp_path))
        return cfg

    def test_every_destination_gets_the_files(self, tmp_path, uploads):
        onedrive = tmp_path / "OneDrive"
        code = run_sync(self._cfg(tmp_path), remote=["s3:bucket/x", str(onedrive)], delete_local=False)
        assert code == 0 and uploads == ["s3:bucket/x", str(onedrive)]

    def test_local_files_are_deleted_only_after_all_destinations_succeed(self, tmp_path, uploads):
        cfg = self._cfg(tmp_path)
        run_sync(cfg, remote=["s3:a", "s3:b"], delete_local=True)
        assert list((tmp_path / "backups" / "postgres" / "app").glob("*.dump")) == []

    def test_a_failing_destination_keeps_local_files_but_the_others_still_run(self, tmp_path, uploads):
        cfg = self._cfg(tmp_path)
        code = run_sync(cfg, remote=["s3:FAIL", "s3:ok"], delete_local=True)
        assert code == 1 and uploads == ["s3:ok"]
        assert len(list((tmp_path / "backups" / "postgres" / "app").glob("*.dump"))) == 2

    def test_dry_run_deletes_nothing(self, tmp_path, uploads):
        cfg = self._cfg(tmp_path)
        run_sync(cfg, remote=["s3:a", "s3:b"], delete_local=True, dry_run=True)
        assert len(list((tmp_path / "backups" / "postgres" / "app").glob("*.dump"))) == 2

    def test_destination_inside_the_backup_folder_is_refused(self, tmp_path, uploads):
        cfg = self._cfg(tmp_path)
        inside = Path(cfg.base_dir) / "mirror"
        with pytest.raises(SystemExit):
            run_sync(cfg, remote=str(inside), delete_local=False, interactive=False)
        assert uploads == []

    def test_single_destination_behaves_as_before(self, tmp_path, uploads):
        cfg = self._cfg(tmp_path)
        assert run_sync(cfg, remote="s3:only", delete_local=False, interactive=False) == 0
        assert uploads == ["s3:only"]

    def test_push_remote_config_with_two_destinations(self, tmp_path, uploads):
        cfg = self._cfg(tmp_path)
        cfg.push_remote = f"s3:a|{tmp_path / 'OneDrive'}"
        run_sync(cfg, delete_local=False, interactive=False)
        assert len(uploads) == 2


class TestLocalTransferFlags:
    def test_network_throttles_are_dropped_for_local_copies(self, tmp_path, monkeypatch):
        seen = {}
        cfg = Config()
        cfg.base_dir = str(_tree(tmp_path))
        monkeypatch.setattr(storage, "rclone_available", lambda: True)
        monkeypatch.setattr(storage, "_run_rclone", lambda args, timeout=0: (0, "", ""))
        monkeypatch.setattr(storage, "_upload", lambda b, r, args, n: seen.update(args=list(args)))
        run_sync(cfg, remote=str(tmp_path / "OneDrive"), delete_local=False, interactive=False)
        assert "--bwlimit" not in seen["args"] and "--tpslimit" not in seen["args"]
        run_sync(cfg, remote="s3:bucket/x", delete_local=False, interactive=False)
        assert "--bwlimit" in seen["args"]


class TestPushInitDestinations:
    def _q(self, texts, confirms, picks=()):
        texts, confirms, picks = iter(texts), iter(confirms), iter(picks)

        class Q:
            class Choice:
                def __init__(self, title, value=None):
                    self.title, self.value = title, value

            def _a(self, it):
                class A:
                    def ask(_self):
                        return next(it)
                return A()

            def text(self, msg, **kw):
                return self._a(texts)

            def confirm(self, msg, **kw):
                return self._a(confirms)

            def select(self, msg, **kw):
                return self._a(picks)
        return Q()

    def test_collects_a_remote_and_a_local_folder(self, tmp_path, monkeypatch):
        import fuelrod_backup.cli as cli_mod

        folder = tmp_path / "OneDrive" / "Backups"
        monkeypatch.setattr("fuelrod_backup.storage.rclone_available", lambda: True)
        monkeypatch.setattr("fuelrod_backup.storage.list_remotes", lambda: ["akilimo-bkp"])
        out = tmp_path / ".backup"
        # prompts, in order: remote path, test connection?, add another?, folder path, create folder?, add another?
        q = self._q(
            texts=["backups", str(folder)],
            confirms=[False, True, True, False],
            picks=["akilimo-bkp", ""],
        )
        cli_mod._save_remote_target(q, out, "PUSH")
        assert out.read_text().strip() == f"PUSH_REMOTE=akilimo-bkp:backups|{folder}"
        assert folder.is_dir()

    def test_keeping_current_destinations_and_adding_none(self, tmp_path, monkeypatch):
        import fuelrod_backup.cli as cli_mod

        monkeypatch.setattr("fuelrod_backup.storage.rclone_available", lambda: False)
        out = tmp_path / ".backup"
        out.write_text("PUSH_REMOTE=a:x|b:y\n")
        q = self._q(texts=[""], confirms=[True, False])  # keep current, then do not add another
        cli_mod._save_remote_target(q, out, "PUSH")
        assert "PUSH_REMOTE=a:x|b:y" in out.read_text()
