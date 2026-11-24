"""init must merge into an existing config instead of rewriting it from scratch."""

from __future__ import annotations

import fuelrod_backup.cli as cli_mod
from fuelrod_backup.config import _parse_env_file

OLD, NEW = "old-" + "value", "new-" + "value"  # placeholders, not credentials

EXISTING = f"""# my own notes
DB_TYPE=postgres
DB_USERNAME=old_user
DB_PASSWORD={OLD}
DB_HOST=db.internal
DB_PORT=5432

# retention and friends are not asked by the wizard
RETENTION=daily=7,weekly=4
NOTIFY_ON=failure
NOTIFY_URL=https://hooks.example.com/x
PULL_REMOTE=akilimo-bkp:db-backup
PUSH_REMOTE=akilimo-bkp:backups|D:/OneDrive/Backups
PUSH_QUOTA=50G
export ENCRYPT=true
BASE_DIR=/old
"""

WIZARD_LINES = [
    "# fuelrod-backup configuration",
    "# Updated by: fuelrod-backup init",
    "",
    "DB_TYPE=postgres",
    "DB_USERNAME=new_user",
    f"DB_PASSWORD={NEW}",
    "DB_HOST=db.internal",
    "DB_PORT=5432",
    "BASE_DIR=/new",
    "KEEP_DAYS=14",
    "",
]


def test_wizard_values_are_updated_and_everything_else_is_preserved(tmp_path):
    cfg = tmp_path / ".backup"
    cfg.write_text(EXISTING)
    cli_mod._write_config(cfg, WIZARD_LINES, updating=True)

    raw = _parse_env_file(cfg)
    assert raw["DB_USERNAME"] == "new_user" and raw["DB_PASSWORD"] == NEW and raw["BASE_DIR"] == "/new"
    assert raw["KEEP_DAYS"] == "14"  # a new key is added
    for key, value in {
        "RETENTION": "daily=7,weekly=4",
        "NOTIFY_ON": "failure",
        "NOTIFY_URL": "https://hooks.example.com/x",
        "PULL_REMOTE": "akilimo-bkp:db-backup",
        "PUSH_REMOTE": "akilimo-bkp:backups|D:/OneDrive/Backups",
        "PUSH_QUOTA": "50G",
    }.items():
        assert raw[key] == value, key
    text = cfg.read_text()
    assert "# my own notes" in text and "export ENCRYPT=true" in text
    assert "old_user" not in text and "/old" not in text


def test_existing_keys_keep_their_position(tmp_path):
    cfg = tmp_path / ".backup"
    cfg.write_text(EXISTING)
    cli_mod._write_config(cfg, WIZARD_LINES, updating=True)
    lines = cfg.read_text().splitlines()
    assert lines.index("DB_USERNAME=new_user") < lines.index("RETENTION=daily=7,weekly=4")
    assert lines[0] == "# my own notes"


def test_previous_file_is_saved_as_bak(tmp_path):
    cfg = tmp_path / ".backup"
    cfg.write_text(EXISTING)
    cli_mod._write_config(cfg, WIZARD_LINES, updating=True)
    assert (tmp_path / ".backup.bak").read_text() == EXISTING


def test_a_new_file_is_written_in_full_without_a_backup(tmp_path):
    cfg = tmp_path / "sub" / ".backup"
    cli_mod._write_config(cfg, WIZARD_LINES, updating=False)
    assert cfg.read_text().startswith("# fuelrod-backup configuration")
    assert not (tmp_path / "sub" / ".backup.bak").exists()


def test_rerunning_with_the_same_answers_changes_nothing(tmp_path):
    cfg = tmp_path / ".backup"
    cfg.write_text(EXISTING)
    cli_mod._write_config(cfg, WIZARD_LINES, updating=True)
    once = cfg.read_text()
    cli_mod._write_config(cfg, WIZARD_LINES, updating=True)
    assert cfg.read_text() == once


def test_removed_keys_are_deleted(tmp_path):
    cfg = tmp_path / ".backup"
    cfg.write_text("PG_HOST=a\nPG_PASSWORD=x\nMY_HOST=b\nRETENTION=daily=3\n")
    cli_mod._write_config(cfg, ["BASE_DIR=/x"], updating=True, remove={"MY_HOST"})
    raw = _parse_env_file(cfg)
    assert "MY_HOST" not in raw and raw["PG_HOST"] == "a" and raw["RETENTION"] == "daily=3"


def test_update_config_keys_handles_export_and_duplicates(tmp_path):
    cfg = tmp_path / ".backup"
    cfg.write_text("export A=1\nA=2\nB=3\n")
    cli_mod._update_config_keys(cfg, {"A": "9"})
    assert cfg.read_text().splitlines() == ["A=9", "B=3"]


def test_blank_lines_are_not_piled_up_when_appending(tmp_path):
    cfg = tmp_path / ".backup"
    cfg.write_text("A=1\n")
    cli_mod._update_config_keys(cfg, {"B": "2", "C": "3"})
    assert cfg.read_text() == "A=1\n\nB=2\nC=3\n"


def test_deselected_engine_credentials_are_removed_by_the_multi_engine_wizard(tmp_path, monkeypatch):
    """Drive the multi-engine wizard: postgres + mariadb exist, only postgres is kept."""
    cfg = tmp_path / ".backup"
    cfg.write_text(
        "PG_USERNAME=u\nPG_PASSWORD=p\nPG_HOST=h\nPG_PORT=5432\nPG_DUMP_CMD=pg_dump\n"
        "MY_USERNAME=u\nMY_PASSWORD=p\nMY_HOST=h\nMY_PORT=3306\nRETENTION=daily=3\n"
    )
    captured = {}
    monkeypatch.setattr(
        cli_mod, "_write_config",
        lambda output, lines, updating, remove=None: captured.update(remove=remove),
    )

    class Q:
        class Choice:
            def __init__(self, title, value=None, checked=False):
                self.title, self.value = title, value

        def _a(self, value):
            class A:
                def ask(_self):
                    return value
            return A()

        def checkbox(self, msg, **kw):
            return self._a(["postgres"])

        def text(self, msg, **kw):
            return self._a(kw.get("default", ""))

        def password(self, msg, **kw):
            return self._a(kw.get("default", ""))

        def confirm(self, msg, **kw):
            return self._a(False if "off-site" in msg else True)

        def select(self, msg, **kw):
            return self._a(kw.get("default"))

    from fuelrod_backup.config import _parse_env_file as parse

    cli_mod._init_multi_engine(Q(), cfg, True, parse(cfg))
    assert captured["remove"] == {"MY_USERNAME", "MY_PASSWORD", "MY_HOST", "MY_PORT"}
