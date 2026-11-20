"""Minimal subprocess environments — the parent environment is never inherited wholesale."""

from __future__ import annotations

import os

_ALLOWED_ENV = (
    "PATH", "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA",
    "SystemRoot", "SYSTEMROOT", "TEMP", "TMP", "LANG",
    "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "DOCKER_CERT_PATH", "DOCKER_TLS_VERIFY",
)


def minimal_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Allowlisted OS variables plus *extra* (e.g. a DB password)."""
    env = {key: os.environ[key] for key in _ALLOWED_ENV if key in os.environ}
    if extra:
        env.update(extra)
    return env
