"""Tests for the verbose (-v / -vv) rclone logging on pull and push."""

from __future__ import annotations

import sys

import pytest

from fuelrod_backup import storage
from fuelrod_backup.storage import mask_args, run_rclone_process, set_verbose, verbose_level

SCRIPT = (
    "import sys; print('listing'); "
    "sys.stderr.write('INFO : a.dump: Copied (new)' + chr(10)); "
    "sys.stderr.write('args=' + ' '.join(sys.argv[1:]) + chr(10)); sys.exit(3)"
)


@pytest.fixture(autouse=True)
def _reset():
    yield
    set_verbose(0)
    storage.use_rclone(None)


@pytest.fixture
def logged(monkeypatch):
    lines: list[str] = []
    monkeypatch.setattr(storage, "log_line", lines.append)
    storage.use_rclone(sys.executable)  # stand in for rclone: python -c <script>
    return lines


class TestMask:
    def test_secret_values_are_hidden(self):
        args = ["copy", "a", "b", "--s3-secret-access-key", "S3CR3T", "--s3-access-key-id", "AKIA", "--x=1"]
        shown = " ".join(mask_args(args))
        assert "S3CR3T" not in shown and "AKIA" not in shown and "--x=1" in shown

    def test_equals_form_is_hidden_too(self):
        assert mask_args(["--pass=" + "x" * 6]) == ["--pass=***"]


class TestRunRcloneProcess:
    def test_quiet_mode_logs_nothing_and_returns_output(self, logged):
        result = run_rclone_process(["-c", SCRIPT], 30)
        assert result.returncode == 3 and b"listing" in result.stdout and b"Copied" in result.stderr
        assert logged == []

    def test_verbose_streams_the_command_and_rclone_output(self, logged):
        set_verbose(1)
        result = run_rclone_process(["-c", SCRIPT], 30, label="postgres/app")
        assert result.returncode == 3 and b"listing" in result.stdout  # output is still captured
        assert logged[0].startswith("[postgres/app] $ ")
        assert any("[postgres/app] " in ln and "Copied (new)" in ln for ln in logged)

    def test_credentials_never_appear_in_the_log(self, logged):
        set_verbose(2)
        run_rclone_process(["-c", SCRIPT, "--s3-secret-access-key", "S3CR3T"], 30)
        assert logged and not any("S3CR3T" in ln for ln in logged)
        assert any("args=" in ln and "***" in ln for ln in logged)

    def test_transfer_commands_get_verbose_flags(self):
        set_verbose(1)
        assert storage._verbose_flags("copy") == ["-v", "--stats", "5s"]
        set_verbose(2)
        assert storage._verbose_flags("check")[0] == "-vv"
        assert storage._verbose_flags("lsjson") == []
        set_verbose(0)
        assert storage._verbose_flags("copy") == []

    def test_level_is_clamped(self):
        set_verbose(-3)
        assert verbose_level() == 0


class TestPullAndPushLogging:
    def test_pull_reports_what_it_will_download_and_why_it_skips(self, tmp_path, monkeypatch):
        import json
        import time
        from types import SimpleNamespace

        from fuelrod_backup import pull, pullhistory
        from fuelrod_backup.config import Config

        lines: list[str] = []
        monkeypatch.setattr(pull, "log_line", lines.append)
        monkeypatch.setattr(pull, "rclone_available", lambda: True)
        monkeypatch.setattr(pullhistory, "default_path", lambda: tmp_path / "history.json")
        listing = {"r:b": "postgres/\n", "r:b/postgres": "app/\n"}

        def fake(args, timeout=None, label=""):
            if args[0] == "lsf":
                return SimpleNamespace(returncode=0, stdout=listing.get(args[2], "").encode(), stderr=b"")
            if args[0] == "lsjson":
                files = [{"Path": n, "Size": 5, "ModTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
                         for n in ("a.dump", "b.dump")]
                return SimpleNamespace(returncode=0, stdout=json.dumps(files).encode(), stderr=b"")
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

        monkeypatch.setattr(storage, "run_rclone_process", fake)
        monkeypatch.setattr(pull, "run_rclone_process", fake)
        local = tmp_path / "local" / "postgres" / "app"
        local.mkdir(parents=True)
        (local / "a.dump").write_bytes(b"12345")
        cfg = Config()
        cfg.base_dir = str(tmp_path / "local")
        set_verbose(2)
        pull.run_pull(cfg, remote="r:b")
        text = "\n".join(lines)
        assert "2 file(s) on the server, 2 selected, 1 to download" in text
        assert "download b.dump" in text and "skip a.dump: already in the local folder" in text

    def test_verbose_upload_streams_instead_of_hiding_stderr(self, tmp_path, monkeypatch):
        seen = {}
        def fake_process(args, timeout, label=""):
            seen["args"] = args
            return storage.subprocess.CompletedProcess(args, 0, b"", b"")

        monkeypatch.setattr(storage, "run_rclone_process", fake_process)
        set_verbose(1)
        storage._upload(tmp_path, "s3:b/x", ["--transfers", "4"], 1)
        assert seen["args"][0] == "copy" and "--progress" not in seen["args"]

    def test_verbose_upload_failure_still_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            storage, "run_rclone_process",
            lambda args, timeout, label="": storage.subprocess.CompletedProcess(args, 1, b"", b"ERROR : denied\n"),
        )
        set_verbose(1)
        with pytest.raises(storage.StorageError, match="denied"):
            storage._upload(tmp_path, "s3:b/x", [], 1)
