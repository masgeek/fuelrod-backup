"""Pre/post backup lifecycle hooks (operator-supplied commands, minimal env, hard timeout)."""

from __future__ import annotations

import os
import shlex
import subprocess

from .procenv import minimal_env

HOOK_TIMEOUT = 300


class HookError(RuntimeError):
    """A hook that must succeed did not."""


def run_hook(command: str, event: str, context: dict[str, str]) -> None:
    """Run *command*; raise HookError on failure. Context is exposed as FUELROD_* env vars."""
    if not command:
        return
    env = minimal_env({f"FUELROD_{k.upper()}": str(v) for k, v in {"event": event, **context}.items()})
    try:
        result = subprocess.run(  # noqa: S603 — operator's own config, list form, no shell
            shlex.split(command, posix=os.name != "nt"), env=env, timeout=HOOK_TIMEOUT, check=False
        )
    except subprocess.TimeoutExpired:
        raise HookError(f"{event} hook timed out after {HOOK_TIMEOUT}s") from None
    except (OSError, ValueError) as exc:
        raise HookError(f"{event} hook failed to start: {exc}") from exc
    if result.returncode != 0:
        raise HookError(f"{event} hook exited with code {result.returncode}")
