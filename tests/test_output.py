"""Tests for machine-readable (JSON) output mode."""

from __future__ import annotations

import io
import json

import pytest
from rich.console import Console

from fuelrod_backup import output
from fuelrod_backup.output import emit, enable_json, json_enabled, register


@pytest.fixture(autouse=True)
def _reset_mode():
    """Keep the module-level JSON flag out of other tests."""
    output._consoles.clear()
    output._json_mode = False
    yield
    output._consoles.clear()
    output._json_mode = False


def _console() -> tuple[Console, io.StringIO]:
    buf = io.StringIO()
    return register(Console(file=buf, width=200)), buf


class TestJsonMode:
    def test_disabled_by_default(self):
        assert json_enabled() is False

    def test_enable_redirects_registered_consoles(self, capsys):
        console, buf = _console()
        enable_json(True)
        assert json_enabled() is True
        console.print("human output")
        assert buf.getvalue() == ""

    def test_disable_restores_stdout(self, capsys):
        console, _buf = _console()
        enable_json(True)
        enable_json(False)
        console.print("human output")
        assert "human output" in capsys.readouterr().out

    def test_console_registered_after_enabling_is_silenced(self):
        enable_json(True)
        _late, buf = _console()
        _late.print("late")
        assert buf.getvalue() == ""

    def test_register_returns_the_console(self):
        console, _buf = _console()
        assert register(console) is console


class TestEmit:
    def test_writes_one_json_document(self, capsys):
        emit({"command": "verify", "status": "ok"})
        out = capsys.readouterr().out
        assert json.loads(out) == {"command": "verify", "status": "ok"}

    def test_serialises_non_json_types(self, capsys):
        from pathlib import Path

        emit({"path": Path("C:/backups/app.dump")})
        assert "app.dump" in capsys.readouterr().out

    def test_human_output_stays_out_of_stdout_in_json_mode(self, capsys):
        console, _buf = _console()
        enable_json(True)
        console.print("progress noise")
        emit({"command": "backup", "status": "ok"})
        out = capsys.readouterr().out
        assert "progress noise" not in out
        assert json.loads(out)["command"] == "backup"
