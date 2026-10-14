"""Tests for notifications and the exit-code contract."""

from __future__ import annotations

import json
import subprocess
from unittest.mock import MagicMock

import pytest

from fuelrod_backup import notify as notify_mod
from fuelrod_backup.config import Config, DbType
from fuelrod_backup.notify import (
    EXIT_ABORTED,
    EXIT_FAILURE,
    EXIT_OK,
    EXIT_USAGE,
    EXIT_VERIFY_FAILED,
    exit_for,
    notify,
    parse_events,
    wants,
)


def _cfg(**kw) -> Config:
    c = Config()
    c.db_type = DbType.POSTGRES
    for k, v in kw.items():
        setattr(c, k, v)
    return c


# ──────────────────────────────────────────────────────────────────────────────
#  Exit-code contract
# ──────────────────────────────────────────────────────────────────────────────

class TestExitCodes:
    def test_contract_values_are_distinct(self):
        codes = [EXIT_OK, EXIT_FAILURE, EXIT_USAGE, EXIT_VERIFY_FAILED, EXIT_ABORTED]
        assert sorted(codes) == [0, 1, 2, 3, 4]

    @pytest.mark.parametrize("status,expected", [
        ("success", EXIT_OK),
        ("failure", EXIT_FAILURE),
        ("aborted", EXIT_ABORTED),
    ])
    def test_exit_for(self, status, expected):
        assert exit_for(status) == expected

    def test_abort_is_not_success(self):
        # The regression this exists for: declining a confirmation used to
        # exit 0, which told automation the run succeeded.
        assert exit_for("aborted") != EXIT_OK


# ──────────────────────────────────────────────────────────────────────────────
#  Event parsing
# ──────────────────────────────────────────────────────────────────────────────

class TestEvents:
    @pytest.mark.parametrize("raw,expected", [
        ("success", {"success"}),
        ("failure,success", {"failure", "success"}),
        ("all success", {"all", "success"}),
        ("", set()),
        ("nonsense", {"nonsense"}),  # parsed, but matches no event
    ])
    def test_parse(self, raw, expected):
        assert parse_events(raw) == expected

    def test_unknown_tokens_match_nothing(self):
        assert wants("success", "nonsense") is False

    def test_disabled_by_default(self):
        assert wants("success", "") is False

    def test_specific_event(self):
        assert wants("failure", "failure") is True
        assert wants("success", "failure") is False

    def test_all_matches_everything(self):
        assert wants("success", "all") is True
        assert wants("failure", "all") is True


# ──────────────────────────────────────────────────────────────────────────────
#  Webhook
# ──────────────────────────────────────────────────────────────────────────────

class TestWebhook:
    def test_posts_json(self, monkeypatch):
        seen = {}

        class FakeResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def fake_urlopen(request, timeout=None):
            seen["url"] = request.full_url
            seen["body"] = json.loads(request.data)
            seen["method"] = request.method
            seen["timeout"] = timeout
            return FakeResponse()

        monkeypatch.setattr(notify_mod.urllib.request, "urlopen", fake_urlopen)
        cfg = _cfg(notify_on="success", notify_url="https://example.test/hook", notify_timeout=7)

        errors = notify(status="success", command="backup", cfg=cfg, summary="2 databases")

        assert errors == []
        assert seen["url"] == "https://example.test/hook"
        assert seen["method"] == "POST"
        assert seen["timeout"] == 7
        assert seen["body"]["command"] == "backup"
        assert seen["body"]["summary"] == "2 databases"
        assert seen["body"]["status"] == "success"

    def test_network_failure_is_reported_not_raised(self, monkeypatch):
        def boom(request, timeout=None):
            raise OSError("connection refused")

        monkeypatch.setattr(notify_mod.urllib.request, "urlopen", boom)
        cfg = _cfg(notify_on="success", notify_url="https://example.test/hook")

        errors = notify(status="success", command="backup", cfg=cfg)

        assert len(errors) == 1 and "connection refused" in errors[0]

    def test_http_error_is_reported(self, monkeypatch):
        class FakeResponse:
            status = 503

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        monkeypatch.setattr(notify_mod.urllib.request, "urlopen", lambda r, timeout=None: FakeResponse())
        cfg = _cfg(notify_on="success", notify_url="https://example.test/hook")

        errors = notify(status="success", command="backup", cfg=cfg)

        assert "503" in errors[0]

    def test_non_http_scheme_is_refused(self):
        cfg = _cfg(notify_on="success", notify_url="file:///etc/passwd")
        errors = notify(status="success", command="backup", cfg=cfg)
        assert "unsupported webhook scheme" in errors[0]

    def test_nothing_is_sent_when_not_configured(self, monkeypatch):
        def explode(request, timeout=None):  # pragma: no cover - must not run
            raise AssertionError("no webhook configured")

        monkeypatch.setattr(notify_mod.urllib.request, "urlopen", explode)
        assert notify(status="success", command="backup", cfg=_cfg()) == []


# ──────────────────────────────────────────────────────────────────────────────
#  Command hooks
# ──────────────────────────────────────────────────────────────────────────────

class TestHooks:
    def test_success_hook_runs_with_placeholders(self, monkeypatch, tmp_path):
        calls = {}

        def fake_run(cmd, **kwargs):
            calls["cmd"] = cmd
            calls["env"] = kwargs.get("env")
            return MagicMock(returncode=0)

        monkeypatch.setattr(notify_mod.subprocess, "run", fake_run)
        cfg = _cfg(notify_on="success", notify_hook_success="/bin/notify --status {status} --db {engine}")

        errors = notify(status="success", command="backup", cfg=cfg, summary="ok")

        assert errors == []
        assert "--status" in calls["cmd"] and "success" in calls["cmd"]
        assert calls["cmd"][-2:] == ["--db", "postgres"]
        assert calls["env"]["FUELROD_STATUS"] == "success"
        assert calls["env"]["FUELROD_COMMAND"] == "backup"

    def test_failure_hook_is_distinct(self, monkeypatch):
        calls = {}
        monkeypatch.setattr(
            notify_mod.subprocess, "run",
            lambda cmd, **kw: calls.setdefault("cmd", cmd) or MagicMock(returncode=0),
        )
        cfg = _cfg(notify_on="failure", notify_hook_failure="/bin/alert {status} {command}")

        notify(status="failure", command="backup", cfg=cfg)

        assert calls["cmd"][:2] == ["/bin/alert", "failure"]

    def test_hook_env_excludes_credentials(self, monkeypatch):
        monkeypatch.setenv("DB_PASSWORD", "super-secret")
        seen = {}
        monkeypatch.setattr(
            notify_mod.subprocess, "run",
            lambda cmd, **kw: seen.update(env=kw.get("env")) or MagicMock(returncode=0),
        )
        cfg = _cfg(notify_on="success", notify_hook_success="/bin/x")

        notify(status="success", command="backup", cfg=cfg)

        assert "DB_PASSWORD" not in seen["env"]

    def test_bad_template_is_reported(self):
        cfg = _cfg(notify_on="success", notify_hook_success="/bin/x {nonexistent}")
        errors = notify(status="success", command="backup", cfg=cfg)
        assert "template error" in errors[0]

    def test_timeout_is_reported(self, monkeypatch):
        def timeout(cmd, **kwargs):
            raise subprocess.TimeoutExpired(cmd, 5)

        monkeypatch.setattr(notify_mod.subprocess, "run", timeout)
        cfg = _cfg(notify_on="success", notify_hook_success="/bin/x")

        errors = notify(status="success", command="backup", cfg=cfg)

        assert "timed out" in errors[0]

    def test_unstartable_hook_is_reported(self, monkeypatch):
        def missing(cmd, **kwargs):
            raise FileNotFoundError("no such file")

        monkeypatch.setattr(notify_mod.subprocess, "run", missing)
        cfg = _cfg(notify_on="success", notify_hook_success="/bin/does-not-exist")

        errors = notify(status="success", command="backup", cfg=cfg)

        assert "failed to start" in errors[0]
