"""Shared pytest fixtures."""

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
