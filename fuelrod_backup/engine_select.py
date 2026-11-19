"""Interactive database-engine selection shared by all commands."""

from __future__ import annotations

from pathlib import Path

from . import prompt as questionary
from .config import Config, DbType, load_all_configs, load_config


def available_engines(base_dir: str | Path) -> list[str]:
    """Engine folders (postgres / mariadb / mssql) that exist directly under *base_dir*."""
    base = Path(base_dir)
    return [e.value for e in DbType if (base / e.value).is_dir() and not (base / e.value).is_symlink()]


def candidate_engines(cfg: Config, config_file: Path | None) -> list[str]:
    """Engines the user could mean: backup folders, prefixed credentials, and the configured one."""
    found = set(available_engines(cfg.base_dir)) if cfg.base_dir else set()
    found.update(c.db_type.value for c in load_all_configs(config_file))
    found.add(cfg.db_type.value)
    return [e.value for e in DbType if e.value in found]


def load_config_choosing_engine(
    config_file: Path | None,
    db_type: str | None,
    *,
    interactive: bool,
    loader=load_config,
) -> Config:
    """load_config, but ask which engine to use when none was given and several are available."""
    cfg = loader(config_file, db_type_override=db_type)
    if db_type is not None or not interactive:
        return cfg
    engines = candidate_engines(cfg, config_file)
    if len(engines) < 2:
        return cfg
    choice = questionary.select(
        "Which database engine?", choices=engines, default=cfg.db_type.value
    ).ask()
    if choice and choice != cfg.db_type.value:
        cfg = loader(config_file, db_type_override=choice)
    return cfg
