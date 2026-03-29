"""Tests for fuelrod_backup/config.py."""

from pathlib import Path

import pytest

from fuelrod_backup.config import (
    Config,
    DbType,
    _parse_env_file,
    load_config,
)

# ──────────────────────────────────────────────────────────────────────────────
#  Fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _setup(monkeypatch, clean_env, no_config_console):
    """Each test gets a clean env and silent console."""


# ──────────────────────────────────────────────────────────────────────────────
#  _parse_env_file
# ──────────────────────────────────────────────────────────────────────────────

class TestParseEnvFile:
    def _write(self, tmp_path: Path, content: str) -> Path:
        f = tmp_path / ".backup"
        f.write_text(content, encoding="utf-8")
        return f

    def test_basic_key_value(self, tmp_path):
        f = self._write(tmp_path, "DB_HOST=localhost\nDB_PORT=5432\n")
        assert _parse_env_file(f) == {"DB_HOST": "localhost", "DB_PORT": "5432"}

    def test_blank_lines_and_comments_ignored(self, tmp_path):
        f = self._write(tmp_path, "\n# a comment\nKEY=val\n\n# another\n")
        assert _parse_env_file(f) == {"KEY": "val"}

    def test_export_prefix_stripped(self, tmp_path):
        f = self._write(tmp_path, "export DB_HOST=myhost\n")
        assert _parse_env_file(f) == {"DB_HOST": "myhost"}

    def test_double_quoted_value(self, tmp_path):
        f = self._write(tmp_path, 'DB_PASSWORD="my secret"\n')
        assert _parse_env_file(f) == {"DB_PASSWORD": "my secret"}

    def test_single_quoted_value(self, tmp_path):
        f = self._write(tmp_path, "DB_PASSWORD='my secret'\n")
        assert _parse_env_file(f) == {"DB_PASSWORD": "my secret"}

    def test_quoted_value_preserves_inline_hash(self, tmp_path):
        f = self._write(tmp_path, 'KEY="value # not a comment"\n')
        assert _parse_env_file(f) == {"KEY": "value # not a comment"}

    def test_unquoted_inline_comment_stripped(self, tmp_path):
        f = self._write(tmp_path, "KEY=value # this is a comment\n")
        assert _parse_env_file(f) == {"KEY": "value"}

    def test_inline_hash_without_space_not_stripped(self, tmp_path):
        # Inline # without preceding space/tab is part of the value.
        f = self._write(tmp_path, "KEY=value#notacomment\n")
        assert _parse_env_file(f) == {"KEY": "value#notacomment"}

    def test_line_without_equals_skipped(self, tmp_path):
        f = self._write(tmp_path, "NOEQUALS\nKEY=val\n")
        assert _parse_env_file(f) == {"KEY": "val"}

    def test_empty_value(self, tmp_path):
        f = self._write(tmp_path, "KEY=\n")
        assert _parse_env_file(f) == {"KEY": ""}

    def test_empty_file(self, tmp_path):
        f = self._write(tmp_path, "")
        assert _parse_env_file(f) == {}

    def test_value_with_equals_sign(self, tmp_path):
        # Only the first '=' is the separator.
        f = self._write(tmp_path, "KEY=a=b=c\n")
        assert _parse_env_file(f) == {"KEY": "a=b=c"}


# ──────────────────────────────────────────────────────────────────────────────
#  DbType
# ──────────────────────────────────────────────────────────────────────────────

class TestDbType:
    def test_valid_values(self):
        assert DbType("postgres") is DbType.POSTGRES
        assert DbType("mariadb") is DbType.MARIADB
        assert DbType("mssql") is DbType.MSSQL

    def test_is_str(self):
        assert DbType.POSTGRES == "postgres"
        assert DbType.MARIADB == "mariadb"

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            DbType("oracle")


# ──────────────────────────────────────────────────────────────────────────────
#  Config.backup_dir
# ──────────────────────────────────────────────────────────────────────────────

class TestConfigBackupDir:
    def test_appends_db_type(self):
        cfg = Config(base_dir="/backups", db_type=DbType.POSTGRES)
        assert cfg.backup_dir == Path("/backups/postgres")

    def test_mariadb_suffix(self):
        cfg = Config(base_dir="/backups", db_type=DbType.MARIADB)
        assert cfg.backup_dir == Path("/backups/mariadb")

    def test_mssql_suffix(self):
        cfg = Config(base_dir="/backups", db_type=DbType.MSSQL)
        assert cfg.backup_dir == Path("/backups/mssql")


# ──────────────────────────────────────────────────────────────────────────────
#  load_config
# ──────────────────────────────────────────────────────────────────────────────

class TestLoadConfig:
    def _write_config(self, tmp_path: Path, content: str) -> Path:
        f = tmp_path / ".backup"
        f.write_text(content, encoding="utf-8")
        return f

    def test_defaults_when_no_file(self, tmp_path):
        cfg = load_config(tmp_path / ".backup")  # file does not exist
        assert cfg.db_type == DbType.POSTGRES
        assert cfg.user == "postgres"
        assert cfg.port == 5432
        assert cfg.host == "127.0.0.1"
        assert cfg.compress is False
        assert cfg.use_docker is True

    def test_db_type_parsed_from_file(self, tmp_path):
        f = self._write_config(tmp_path, "DB_TYPE=mariadb\n")
        cfg = load_config(f)
        assert cfg.db_type == DbType.MARIADB

    def test_invalid_db_type_falls_back_to_postgres(self, tmp_path):
        f = self._write_config(tmp_path, "DB_TYPE=oracle\n")
        cfg = load_config(f)
        assert cfg.db_type == DbType.POSTGRES

    def test_per_engine_defaults_mariadb(self, tmp_path):
        f = self._write_config(tmp_path, "DB_TYPE=mariadb\n")
        cfg = load_config(f)
        assert cfg.user == "root"
        assert cfg.port == 3306
        assert cfg.service == "mariadb"

    def test_per_engine_defaults_mssql(self, tmp_path):
        f = self._write_config(tmp_path, "DB_TYPE=mssql\n")
        cfg = load_config(f)
        assert cfg.user == "sa"
        assert cfg.port == 1433
        assert cfg.service == "mssql"

    def test_boolean_true_variants(self, tmp_path):
        for val in ("true", "1", "yes", "TRUE", "YES"):
            f = self._write_config(tmp_path, f"COMPRESS_FILE={val}\nUSE_DOCKER={val}\n")
            cfg = load_config(f)
            assert cfg.compress is True, f"COMPRESS_FILE={val!r} should be True"
            assert cfg.use_docker is True, f"USE_DOCKER={val!r} should be True"

    def test_boolean_false_variants(self, tmp_path):
        for val in ("false", "0", "no", "FALSE"):
            f = self._write_config(tmp_path, f"COMPRESS_FILE={val}\nUSE_DOCKER={val}\n")
            cfg = load_config(f)
            assert cfg.compress is False
            assert cfg.use_docker is False

    def test_integer_fields(self, tmp_path):
        f = self._write_config(tmp_path, "KEEP_DAYS=14\nCONNECTION_TIMEOUT=60\n")
        cfg = load_config(f)
        assert cfg.days_to_keep == 14
        assert cfg.connection_timeout == 60

    def test_invalid_integer_falls_back_to_default(self, tmp_path):
        f = self._write_config(tmp_path, "KEEP_DAYS=notanumber\nCONNECTION_TIMEOUT=bad\n")
        cfg = load_config(f)
        assert cfg.days_to_keep == 7
        assert cfg.connection_timeout == 30

    def test_env_var_overrides_file(self, tmp_path, monkeypatch):
        f = self._write_config(tmp_path, "DB_USERNAME=fileuser\n")
        monkeypatch.setenv("DB_USERNAME", "envuser")
        cfg = load_config(f)
        assert cfg.user == "envuser"

    def test_n8n_services_parsed(self, tmp_path):
        f = self._write_config(tmp_path, "N8N_SERVICES=n8n worker\n")
        cfg = load_config(f)
        assert cfg.n8n_services == ["n8n", "worker"]

    def test_skip_services_empty_by_default(self, tmp_path):
        f = self._write_config(tmp_path, "DB_TYPE=postgres\n")
        cfg = load_config(f)
        assert cfg.skip_services == []

    def test_config_source_set(self, tmp_path):
        f = self._write_config(tmp_path, "DB_TYPE=postgres\n")
        cfg = load_config(f)
        assert cfg.config_source == f.resolve()

    def test_config_source_none_when_file_missing(self, tmp_path):
        cfg = load_config(tmp_path / ".backup")
        assert cfg.config_source is None
