"""Every configuration key must be reachable from `init`, and the advanced wizard must merge safely."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import typer

import fuelrod_backup.cli as cli_mod
from fuelrod_backup import settings as st
from fuelrod_backup.config import Config, _load_ops, _load_pull, _parse_env_file

SECRETISH = "placeholder-" + "value"
ROOT = Path(__file__).resolve().parent.parent
SYSTEM_VARIABLES = {"APPDATA", "USERPROFILE", "XDG_CONFIG_HOME", "RCLONE_CONFIG"}  # read from the OS, not the config


class TestEveryKeyHasAWizard:
    def _keys_read_by_config(self) -> set[str]:
        source = (ROOT / "fuelrod_backup" / "config.py").read_text(encoding="utf-8")
        found = set(re.findall(r'\b_?get[c]?\(\s*"([A-Z][A-Z0-9_]+)"', source))
        return found - SYSTEM_VARIABLES

    def test_all_keys_read_in_config_py_are_registered(self):
        missing = sorted(self._keys_read_by_config() - set(st.BY_KEY))
        assert not missing, (
            f"{missing} are read by config.py but missing from settings.SETTINGS — add them so `init` can set them"
        )

    def test_registry_keys_are_unique_and_groups_valid(self):
        keys = [s.key for s in st.SETTINGS]
        assert len(keys) == len(set(keys))
        for s in st.SETTINGS:
            assert s.group in st.SECTIONS or s.group in ("core", "remote"), s.key

    def test_every_setting_is_documented_in_backup_example(self):
        example = (ROOT / ".backup.example").read_text(encoding="utf-8")
        missing = [s.key for s in st.SETTINGS if s.key not in example]
        assert not missing, f"document {missing} in .backup.example"

    def test_every_non_core_setting_names_the_command_that_sets_it(self):
        for s in st.SETTINGS:
            if s.group == "remote":
                assert s.wizard in ("push-init", "pull-init", "sync-init"), s.key
            elif s.advanced:
                assert s.wizard == "init"

    def test_defaults_match_the_config_loader(self):
        cfg = Config()
        _load_ops(cfg, lambda key, default="": default)
        _load_pull(cfg, lambda key, default="": default)
        expected = {
            "RETENTION": cfg.retention, "MANIFEST": cfg.manifest, "SIZE_WARN_RATIO": cfg.size_warn_ratio,
            "PUSH_LATEST": cfg.push_latest, "PUSH_DELETE_LOCAL": cfg.push_delete_local,
            "PUSH_QUOTA_TARGET": cfg.push_quota_target, "PUSH_QUOTA_KEEP": cfg.push_quota_keep,
            "PULL_DELETE_REMOTE": cfg.pull_delete_remote, "PULL_KEEP_REMOTE": cfg.pull_keep_remote,
            "RCLONE_CMD": cfg.rclone_cmd,
        }
        for key, value in expected.items():
            default = st.BY_KEY[key].default
            if isinstance(value, bool):
                assert (default.lower() == "true") is value, key
            elif isinstance(value, float):
                assert float(default) == value, key
            elif isinstance(value, int):
                assert int(default or 0) == value, key
            else:
                assert default == value, key


class TestNormalise:
    def test_bool_int_choice_and_ranges(self):
        assert st.normalise(st.BY_KEY["MANIFEST"], "Yes") == "true"
        with pytest.raises(ValueError):
            st.normalise(st.BY_KEY["MANIFEST"], "maybe")
        assert st.normalise(st.BY_KEY["PUSH_LATEST"], " 5 ") == "5"
        for bad in ("x", "-1"):
            with pytest.raises(ValueError):
                st.normalise(st.BY_KEY["PUSH_LATEST"], bad)
        with pytest.raises(ValueError):
            st.normalise(st.BY_KEY["PUSH_QUOTA_TARGET"], "101")
        with pytest.raises(ValueError):
            st.normalise(st.BY_KEY["NOTIFY_ON"], "sometimes")

    def test_validators(self):
        assert st.normalise(st.BY_KEY["RETENTION"], "daily=7,weekly=4") == "daily=7,weekly=4"
        with pytest.raises(ValueError):
            st.normalise(st.BY_KEY["RETENTION"], "hourly=3")
        assert st.normalise(st.BY_KEY["PUSH_QUOTA"], "50G") == "50G"
        with pytest.raises(ValueError):
            st.normalise(st.BY_KEY["PUSH_QUOTA"], "lots")
        with pytest.raises(ValueError):
            st.normalise(st.BY_KEY["NOTIFY_URL"], "ftp://x")
        assert st.normalise(st.BY_KEY["NOTIFY_URL"], "") == ""

    def test_dependencies(self):
        assert not st.applies(st.BY_KEY["ENCRYPT_TOOL"], {})
        assert st.applies(st.BY_KEY["ENCRYPT_TOOL"], {"ENCRYPT": "true"})
        assert not st.applies(st.BY_KEY["NOTIFY_URL"], {"NOTIFY_ON": ""})
        assert st.applies(st.BY_KEY["NOTIFY_URL"], {"NOTIFY_ON": "failure"})
        assert st.applies(st.BY_KEY["PUSH_QUOTA_TARGET"], {"PUSH_QUOTA": "50G"})
        assert not st.applies(st.BY_KEY["PULL_KEEP_REMOTE"], {})


class FakeQ:
    """Scripted prompts keyed by the setting key that appears in the prompt text."""

    class Choice:
        def __init__(self, title, value=None, checked=False):
            self.title, self.value = title, value

    def __init__(self, answers: dict[str, object], sections=None, save=True):
        self.answers, self.sections, self.save = answers, sections, save
        self.asked: list[str] = []

    def _a(self, value):
        class A:
            def ask(_self):
                return value
        return A()

    def _key(self, msg: str):
        m = re.search(r"\(([A-Z][A-Z0-9_]+)\)", msg)
        return m.group(1) if m else None

    def confirm(self, msg, **kw):
        if msg.startswith("Save these changes"):
            return self._a(self.save)
        key = self._key(msg)
        self.asked.append(key)
        return self._a(self.answers.get(key, kw.get("default")))

    def text(self, msg, **kw):
        key = self._key(msg)
        self.asked.append(key)
        value = self.answers.get(key, kw.get("default", ""))
        if isinstance(value, list):  # successive answers, for retry tests
            value = value.pop(0)
        return self._a(value)

    def select(self, msg, **kw):
        key = self._key(msg)
        self.asked.append(key)
        return self._a(self.answers.get(key, kw.get("default")))

    def password(self, msg, **kw):
        key = self._key(msg)
        self.asked.append(key)
        return self._a(self.answers.get(key, ""))

    def checkbox(self, msg, **kw):
        return self._a(self.sections)


class TestAdvancedWizard:
    def test_answers_are_merged_and_everything_else_is_kept(self, tmp_path):
        cfg = tmp_path / ".backup"
        cfg.write_text("# mine\nDB_TYPE=postgres\nDB_PASSWORD=secret\nPULL_REMOTE=akilimo-bkp:db\n")
        q = FakeQ({"RETENTION": "daily=7,weekly=4", "PUSH_QUOTA": "50G", "NOTIFY_ON": "failure",
                   "NOTIFY_URL": "https://hooks.example.com/x"})
        cli_mod._run_advanced(q, cfg, ["storage", "quota", "notifications"])
        raw = _parse_env_file(cfg)
        assert raw["RETENTION"] == "daily=7,weekly=4" and raw["PUSH_QUOTA"] == "50G"
        assert raw["NOTIFY_ON"] == "failure" and raw["NOTIFY_URL"] == "https://hooks.example.com/x"
        assert raw["DB_PASSWORD"] == "secret" and raw["PULL_REMOTE"] == "akilimo-bkp:db"
        assert "# mine" in cfg.read_text()

    def test_defaults_are_not_written_to_the_file(self, tmp_path):
        cfg = tmp_path / ".backup"
        cfg.write_text("DB_TYPE=postgres\n")
        cli_mod._run_advanced(FakeQ({}), cfg, ["push", "pull", "storage"])
        assert cfg.read_text() == "DB_TYPE=postgres\n"  # nothing changed: all answers were the defaults

    def test_only_applicable_settings_are_asked(self, tmp_path):
        cfg = tmp_path / ".backup"
        q = FakeQ({"ENCRYPT": False})
        cli_mod._run_advanced(q, cfg, ["encryption"])
        assert q.asked == ["ENCRYPT"]  # tool / recipient / files are skipped while encryption is off
        q = FakeQ({"ENCRYPT": True, "ENCRYPT_TOOL": "gpg", "ENCRYPT_RECIPIENT": "me@example.com"})
        cli_mod._run_advanced(q, cfg, ["encryption"])
        raw = _parse_env_file(cfg)
        assert (raw["ENCRYPT"], raw["ENCRYPT_TOOL"]) == ("true", "gpg")
        assert raw["ENCRYPT_RECIPIENT"] == "me@example.com"

    def test_invalid_answers_are_asked_again(self, tmp_path):
        cfg = tmp_path / ".backup"
        q = FakeQ({"PUSH_QUOTA": ["lots", "50G"]})
        cli_mod._run_advanced(q, cfg, ["quota"])
        assert _parse_env_file(cfg)["PUSH_QUOTA"] == "50G"

    def test_clearing_an_existing_value_removes_the_key(self, tmp_path):
        cfg = tmp_path / ".backup"
        cfg.write_text("RETENTION=daily=3\nBASE_DIR=/x\n")
        cli_mod._run_advanced(FakeQ({"RETENTION": ""}), cfg, ["storage"])
        raw = _parse_env_file(cfg)
        assert "RETENTION" not in raw and raw["BASE_DIR"] == "/x"

    def test_secret_blank_keeps_the_current_value(self, tmp_path):
        setting = st.BY_KEY["DB_PASSWORD"]
        assert cli_mod._ask_setting(FakeQ({}), setting, "old") is None

    def test_declining_the_save_changes_nothing(self, tmp_path):
        cfg = tmp_path / ".backup"
        cfg.write_text("A=1\n")
        with pytest.raises(Exception):  # noqa: B017 — typer.Exit
            cli_mod._run_advanced(FakeQ({"PUSH_LATEST": "9"}, save=False), cfg, ["push"])
        assert cfg.read_text() == "A=1\n"

    def test_picker_when_no_section_is_given(self, tmp_path):
        cfg = tmp_path / ".backup"
        q = FakeQ({"PUSH_LATEST": "9"}, sections=["push"])
        cli_mod._run_advanced(q, cfg, None)
        assert _parse_env_file(cfg)["PUSH_LATEST"] == "9"

    def test_no_selection_is_a_no_op(self, tmp_path):
        cfg = tmp_path / ".backup"
        cli_mod._run_advanced(FakeQ({}, sections=[]), cfg, None)
        assert not cfg.exists()

    def test_bool_values_are_stored_as_true_false(self, tmp_path):
        cfg = tmp_path / ".backup"
        cli_mod._run_advanced(FakeQ({"PUSH_DELETE_LOCAL": False, "MANIFEST": False}), cfg, ["push", "storage"])
        raw = _parse_env_file(cfg)
        assert raw["PUSH_DELETE_LOCAL"] == "false" and raw["MANIFEST"] == "false"


class TestInitCommand:
    def test_list_json_contains_every_setting(self, tmp_path, capsys):
        cfg = tmp_path / ".backup"
        cfg.write_text(f"RETENTION=daily=7\nDB_PASSWORD={SECRETISH}\n")
        cli_mod._print_settings(cfg, as_json=True)
        data = json.loads(capsys.readouterr().out)
        assert {r["key"] for r in data["settings"]} == set(st.BY_KEY)
        by_key = {r["key"]: r for r in data["settings"]}
        assert by_key["RETENTION"]["value"] == "daily=7"
        assert by_key["DB_PASSWORD"]["value"] == "***"  # never printed
        assert by_key["PUSH_QUOTA"]["value"] is None
        assert by_key["PUSH_QUOTA"]["configure_with"] == "init --section quota"
        assert by_key["PULL_REMOTE"]["configure_with"] == "pull-init"

    def test_unknown_section_is_a_usage_error(self, tmp_path):
        with pytest.raises((SystemExit, typer.Exit)):
            cli_mod.init_config(output=tmp_path / ".backup", section=["nope"], advanced=False,
                                list_settings=False, as_json=False)


class TestDocsStayInSync:
    def test_settings_reference_in_configuration_md_is_current(self):
        doc = (ROOT / "docs" / "CONFIGURATION.md").read_text(encoding="utf-8")
        block = doc.split("<!-- settings:start -->")[1].split("<!-- settings:end -->")[0].strip()
        assert block == st.render_markdown(), "regenerate with: python -m fuelrod_backup.settings"

    def test_every_command_is_in_the_readme_command_list(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        names = {c.name or c.callback.__name__.replace("_", "-") for c in cli_mod.app.registered_commands}
        missing = sorted(n for n in names if f"fuelrod-backup {n}" not in readme)
        assert not missing, f"add {missing} to the CLI Commands list in README.md"

    def test_every_module_is_in_the_architecture_doc(self):
        doc = (ROOT / "docs" / "ARCHITECTURE.md").read_text(encoding="utf-8")
        modules = sorted(p.stem for p in (ROOT / "fuelrod_backup").glob("*.py") if p.stem != "__init__")
        missing = [m for m in modules if f"`{m}.py`" not in doc]
        assert not missing, f"add {missing} to the module table in docs/ARCHITECTURE.md"
