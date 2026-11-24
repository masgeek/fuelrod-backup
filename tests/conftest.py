"""Shared pytest fixtures."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# All env vars that load_config reads — cleared before each config test so the
# host environment cannot leak into assertions.
_CONFIG_ENV_KEYS = [
    "DB_TYPE", "DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_PORT",
    "USE_DOCKER", "SERVICE", "BASE_DIR", "COMPRESS_FILE", "KEEP_DAYS",
    "CONNECTION_TIMEOUT", "N8N_SERVICES", "SKIP_SERVICES",
    "GDRIVE", "BACKUP_AGE", "INCLUDE_FILES",
    "PSQL_CMD", "PG_DUMP_CMD", "PG_RESTORE_CMD",
    "MYSQL_DUMP_CMD", "MYSQL_CMD", "MSSQL_BACKUP_DIR",
    # Migration-specific tunables
    "MIGRATE_BATCH_SIZE", "MIGRATE_PARALLEL", "MIGRATE_DRY_RUN",
    "MIGRATE_VALIDATE", "MIGRATE_TARGET_SCHEMA", "MIGRATE_UNSIGNED_CHECKS",
]


@pytest.fixture()
def clean_env(monkeypatch):
    """Strip all fuelrod-backup env vars from the test process."""
    for key in _CONFIG_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture()
def no_config_console(monkeypatch):
    """Silence the Rich console that config.py prints to stderr."""
    monkeypatch.setattr("fuelrod_backup.config._console", MagicMock())


@pytest.fixture(autouse=True)
def _no_real_rclone(monkeypatch):
    """
    Tests must never run a real rclone: CI has none, and a developer machine that does would hide tests
    that forgot to fake it. A test that fakes ``subprocess.run`` (or a module's ``_run_rclone``) still works;
    only a call that reaches the real process machinery fails, exactly as it would on a machine without rclone.
    """
    real_run, real_popen = subprocess.run, subprocess.Popen

    def _check(cmd) -> None:
        first = cmd[0] if isinstance(cmd, (list, tuple)) and cmd else cmd
        if isinstance(first, (str, Path)) and Path(str(first)).stem.lower() == "rclone":
            raise FileNotFoundError(2, "rclone is not available in tests", str(first))

    def guarded_run(cmd, *args, **kwargs):
        _check(cmd)
        return real_run(cmd, *args, **kwargs)

    class GuardedPopen(real_popen):
        def __init__(self, cmd, *args, **kwargs):
            _check(cmd)
            super().__init__(cmd, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", guarded_run)
    monkeypatch.setattr(subprocess, "Popen", GuardedPopen)


def patch_rclone(monkeypatch, fake) -> None:
    """Install *fake* as ``_run_rclone`` everywhere it is imported (storage, manifest and quota each bind it)."""
    for module in ("storage", "manifest", "quota"):
        monkeypatch.setattr(f"fuelrod_backup.{module}._run_rclone", fake)
