"""Configuration dataclass and config file parser (.backup / .env)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from rich.console import Console as _Console

_console = _Console(stderr=True)


class DbType(StrEnum):
    POSTGRES = "postgres"
    MARIADB = "mariadb"
    MSSQL = "mssql"


# Maps each engine to the config key prefix used in multi-engine mode
_ENGINE_PREFIXES: dict[str, str] = {
    DbType.POSTGRES: "PG_",
    DbType.MARIADB: "MY_",
    DbType.MSSQL: "MS_",
}


@dataclass
class Config:
    user: str = "postgres"
    password: str = ""
    host: str = "127.0.0.1"
    port: int = 5432
    service: str = "postgres"  # Docker container name
    use_docker: bool = True
    base_dir: str = ""  # backup root directory (raw root)
    compress: bool = False
    days_to_keep: int = 7
    connection_timeout: int = 30  # seconds; applies to driver connect + docker subprocess checks
    # n8n volume backup
    n8n_services: list[str] = field(default_factory=lambda: ["n8n"])
    skip_services: list[str] = field(default_factory=lambda: [])
    psql_cmd: str = "psql"
    pg_dump_cmd: str = "pg_dump"
    pg_restore_cmd: str = "pg_restore"
    # Engine selector
    db_type: DbType = DbType.POSTGRES
    # MariaDB / MySQL specific
    mysql_dump_cmd: str = "mariadb-dump"
    mysql_cmd: str = "mysql"
    # MSSQL specific
    mssql_backup_dir: str = "/var/opt/mssql/backups"  # path inside container
    # Google Drive sync (gbk / rclone)
    gdrive_remote: str = "db-backup"          # GDRIVE — rclone remote folder name
    gdrive_age_days: int = 2                  # BACKUP_AGE — prune remote files older than N days
    gdrive_include: list[str] = field(default_factory=lambda: [
        "*.sql.zip", "*.sql.gz", "*_backups.zip", "*.tar.gz",
        "*.dump", "*.dump.gz", "*.bak", "*.txt",
    ])                                        # INCLUDE_FILES — space-separated glob patterns
    config_source: Path | None = field(default=None, repr=False)  # which file was loaded

    @property
    def backup_dir(self) -> Path:
        """
        Return the effective backup directory with db_type suffix appended.
        Example: /backups/postgres, /backups/mariadb, /backups/mssql
        """
        return Path(self.base_dir) / self.db_type.value


def _parse_env_file(path: Path) -> dict[str, str]:
    """
    Parse a shell-style key=value file (.backup or .env).

    Rules (matches docker-compose / python-dotenv behaviour):
    - Blank lines and lines starting with # are ignored
    - Leading `export ` is stripped
    - Quoted values  ("..." or '...'):  inner content is used verbatim, no comment stripping
    - Unquoted values: trailing whitespace stripped, then inline # comments stripped
      (a space or tab before # is required to distinguish '#' in a value from a comment)
    """
    result: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        line = re.sub(r"^export\s+", "", line)
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()

        if len(val) >= 2 and val[0] in ('"', "'") and val[-1] == val[0]:
            # Quoted value — use content verbatim (no comment stripping inside quotes)
            val = val[1:-1]
        else:
            # Unquoted value — strip trailing inline comment (space/tab + # required)
            val = re.sub(r"[ \t]#.*$", "", val).rstrip()

        result[key] = val
    return result


def _find_config_file() -> Path | None:
    """
    Search for a config file, checking these locations in order:

    1. Current working directory  (.backup then .env)
    2. Package project directory  (.backup then .env)
    3. Repo root / one level up   (.backup then .env)

    Returns the first file found, or None.
    """
    pkg_dir = Path(__file__).parent.parent  # fuelrod-backup/
    repo_root = pkg_dir.parent  # proxy-tool/
    cwd = Path.cwd()

    search_dirs = [cwd, pkg_dir]
    if repo_root != cwd and repo_root != pkg_dir:
        search_dirs.append(repo_root)

    for directory in search_dirs:
        for name in (".backup", ".env", ".env-backup"):
            candidate = directory / name
            if candidate.is_file():
                return candidate

    return None


def load_config(config_file: Path | None = None, db_type_override: str | None = None) -> Config:
    """
    Build a Config by merging (lowest → highest priority):
      1. Dataclass defaults
      2. .backup or .env file (auto-discovered, or explicit --config path)
      3. Environment variables (always win)
      4. db_type_override (CLI --db-type flag, highest priority for engine selection)

    The resolved config_source field records which file was actually used.
    """
    cfg = Config()

    if config_file is None:
        config_file = _find_config_file()

    raw: dict[str, str] = {}
    if config_file and config_file.is_file():
        raw = _parse_env_file(config_file)
        cfg.config_source = config_file.resolve()
        _console.print(f"[dim]config:[/] {cfg.config_source}")
    else:
        _console.print("[dim]config:[/] [yellow]no config file found — using defaults[/]")

    def _get(key: str, default: str = "") -> str:
        return os.environ.get(key, raw.get(key, default))

    # Engine selector — CLI override wins over file/env
    if db_type_override is not None:
        try:
            cfg.db_type = DbType(db_type_override.lower())
        except ValueError:
            cfg.db_type = DbType.POSTGRES
    else:
        try:
            cfg.db_type = DbType(_get("DB_TYPE", "postgres").lower())
        except ValueError:
            cfg.db_type = DbType.POSTGRES

    # Default BASE_DIR: next to the config file if one was found, else cwd/db-backup.
    # Never fall back into site-packages (Path(__file__) is wrong after pip install).
    if config_file:
        _fallback_base = str(config_file.parent / "db-backup")
    else:
        _fallback_base = str(Path.cwd() / "db-backup")

    raw_base = _get("BASE_DIR", _fallback_base)
    cfg.base_dir = str(Path(raw_base))

    # Per-engine defaults for user, port, and service container name
    if cfg.db_type == DbType.MARIADB:
        _default_user, _default_port, _default_service = "root", "3306", "mariadb"
    elif cfg.db_type == DbType.MSSQL:
        _default_user, _default_port, _default_service = "sa", "1433", "mssql"
    else:
        _default_user, _default_port, _default_service = "postgres", "5432", "postgres"

    # For each connection field, fall back to the engine-prefixed key so that
    # a multi-engine config (PG_*/MY_*/MS_*) also works with single-engine commands.
    _pfx = _ENGINE_PREFIXES[cfg.db_type]

    def _getc(generic: str, prefixed_short: str, default: str = "") -> str:
        return _get(generic, "") or _get(_pfx + prefixed_short, default)

    cfg.user = _getc("DB_USERNAME", "USERNAME", _default_user)
    cfg.password = _getc("DB_PASSWORD", "PASSWORD", "")
    cfg.host = _getc("DB_HOST", "HOST", "127.0.0.1")
    cfg.service = _getc("SERVICE", "SERVICE", _default_service)
    cfg.use_docker = _getc("USE_DOCKER", "USE_DOCKER", "true").strip().lower() in ("true", "1", "yes")
    cfg.compress = _get("COMPRESS_FILE", "false").strip().lower() in ("true", "1", "yes")
    try:
        cfg.connection_timeout = int(_get("CONNECTION_TIMEOUT", "30"))
    except ValueError:
        cfg.connection_timeout = 30

    cfg.psql_cmd = _get("PSQL_CMD", "psql")
    cfg.pg_dump_cmd = _get("PG_DUMP_CMD", "pg_dump")
    cfg.pg_restore_cmd = _get("PG_RESTORE_CMD", "pg_restore")

    # MariaDB / MySQL
    cfg.mysql_dump_cmd = _get("MYSQL_DUMP_CMD", "mysqldump")
    cfg.mysql_cmd = _get("MYSQL_CMD", "mysql")

    # MSSQL
    cfg.mssql_backup_dir = _get("MSSQL_BACKUP_DIR", "/var/opt/mssql/backups")

    try:
        cfg.port = int(_getc("DB_PORT", "PORT", _default_port))
    except ValueError:
        cfg.port = int(_default_port)

    try:
        cfg.days_to_keep = int(_get("KEEP_DAYS", "7"))
    except ValueError:
        cfg.days_to_keep = 7

    # n8n volume backup
    raw_n8n = _get("N8N_SERVICES", "n8n").strip()
    cfg.n8n_services = [s.strip() for s in raw_n8n.split() if s.strip()]
    raw_skip = _get("SKIP_SERVICES", "").strip()
    cfg.skip_services = [s.strip() for s in raw_skip.split() if s.strip()] if raw_skip else []

    # Google Drive sync
    cfg.gdrive_remote = _get("GDRIVE", "db-backup")
    try:
        cfg.gdrive_age_days = int(_get("BACKUP_AGE", "2"))
    except ValueError:
        cfg.gdrive_age_days = 2
    raw_include = _get("INCLUDE_FILES", "").strip()
    if raw_include:
        cfg.gdrive_include = [p.strip() for p in raw_include.split() if p.strip()]

    return cfg


def _load_prefixed_config(
    db_type: DbType,
    prefix: str,
    raw: dict[str, str],
    config_file: Path | None,
    fallback_base: str,
) -> Config | None:
    """Build a Config for *db_type* using *prefix*-keyed values (e.g. PG_, MY_, MS_).

    Returns None when neither ``{prefix}USERNAME`` nor ``{prefix}HOST`` is set,
    meaning this engine is not configured for multi-engine mode.
    """
    def _get(key: str, default: str = "") -> str:
        return os.environ.get(key, raw.get(key, default))

    def _ep(short: str, default: str = "") -> str:
        return os.environ.get(prefix + short, raw.get(prefix + short, default))

    if not (_ep("USERNAME") or _ep("HOST")):
        return None

    cfg = Config()
    cfg.db_type = db_type
    cfg.config_source = config_file.resolve() if config_file else None

    # Shared settings (no per-engine prefix)
    cfg.base_dir = str(Path(_get("BASE_DIR", fallback_base)))
    cfg.compress = _get("COMPRESS_FILE", "false").strip().lower() in ("true", "1", "yes")
    try:
        cfg.days_to_keep = int(_get("KEEP_DAYS", "7"))
    except ValueError:
        cfg.days_to_keep = 7
    try:
        cfg.connection_timeout = int(_get("CONNECTION_TIMEOUT", "30"))
    except ValueError:
        cfg.connection_timeout = 30

    # Per-engine connection defaults
    if db_type == DbType.MARIADB:
        _def_user, _def_port, _def_service = "root", "3306", "mariadb"
    elif db_type == DbType.MSSQL:
        _def_user, _def_port, _def_service = "sa", "1433", "mssql"
    else:
        _def_user, _def_port, _def_service = "postgres", "5432", "postgres"

    cfg.user = _ep("USERNAME", _def_user)
    cfg.password = _ep("PASSWORD", "")
    cfg.host = _ep("HOST", "127.0.0.1")
    cfg.service = _ep("SERVICE", _def_service)
    cfg.use_docker = _ep("USE_DOCKER", "true").strip().lower() in ("true", "1", "yes")
    try:
        cfg.port = int(_ep("PORT", _def_port))
    except ValueError:
        cfg.port = int(_def_port)

    if db_type == DbType.POSTGRES:
        cfg.pg_dump_cmd = _ep("DUMP_CMD", "pg_dump")
        cfg.pg_restore_cmd = _ep("RESTORE_CMD", "pg_restore")
        cfg.psql_cmd = _ep("CMD", "psql")
    elif db_type == DbType.MARIADB:
        cfg.mysql_dump_cmd = _ep("DUMP_CMD", "mysqldump")
        cfg.mysql_cmd = _ep("CMD", "mysql")
    elif db_type == DbType.MSSQL:
        cfg.mssql_backup_dir = _ep("BACKUP_DIR", "/var/opt/mssql/backups")

    return cfg


def load_all_configs(config_file: Path | None = None) -> list[Config]:
    """Return one Config per engine that has prefixed keys in the config file.

    Looks for ``PG_USERNAME``/``PG_HOST`` (postgres), ``MY_*`` (mariadb),
    ``MS_*`` (mssql).  Shared settings (``BASE_DIR``, ``COMPRESS_FILE``,
    ``KEEP_DAYS``, ``CONNECTION_TIMEOUT``) are read without a prefix and
    applied to every engine config.

    Falls back to a single :func:`load_config` call when no engine-prefixed
    keys are found (preserves backward-compatibility with ``DB_TYPE`` configs).
    """
    if config_file is None:
        config_file = _find_config_file()

    raw: dict[str, str] = {}
    if config_file and config_file.is_file():
        raw = _parse_env_file(config_file)

    fallback_base = str(
        (config_file.parent / "db-backup") if config_file else (Path.cwd() / "db-backup")
    )

    configs: list[Config] = []
    for db_type_val, prefix in _ENGINE_PREFIXES.items():
        cfg = _load_prefixed_config(DbType(db_type_val), prefix, raw, config_file, fallback_base)
        if cfg is not None:
            configs.append(cfg)

    if not configs:
        # No engine-prefixed keys found — behave like single-engine load_config
        return [load_config(config_file)]

    src = str(config_file.resolve()) if config_file else "[yellow]none[/]"
    _console.print(f"[dim]config:[/] {src}")
    engines = ", ".join(c.db_type.value for c in configs)
    _console.print(f"[dim]engines:[/] {engines}")
    return configs
