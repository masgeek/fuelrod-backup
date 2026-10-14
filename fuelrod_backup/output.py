"""Machine-readable output mode.

When JSON mode is on, every registered Rich console is pointed at a null
stream and commands emit a single structured document on stdout instead of
human-formatted panels. Errors that abort a run are still reported on stderr
via the normal exit-code contract.
"""

from __future__ import annotations

import json
import sys
from typing import Any

_consoles: list[Any] = []
_json_mode = False


class _NullStream:
    """A file-like sink that swallows everything Rich writes."""

    def write(self, text: str) -> int:
        return len(text)

    def flush(self) -> None:
        return None

    def isatty(self) -> bool:
        return False

    def fileno(self) -> int:
        raise OSError("null stream has no file descriptor")


def register(console: Any) -> Any:
    """
    Register a Rich console so JSON mode can silence it.

    Call immediately after creating the module-level console:
        console = Console()
        register(console)
    """
    _consoles.append(console)
    if _json_mode:
        console.file = _NullStream()
    return console


def enable_json(enabled: bool = True) -> None:
    """Switch every registered console between real stdout and the null sink."""
    global _json_mode
    _json_mode = enabled
    for console in _consoles:
        console.file = _NullStream() if enabled else sys.stdout


def json_enabled() -> bool:
    return _json_mode


def emit(payload: dict[str, Any]) -> None:
    """Write one JSON document to stdout."""
    print(json.dumps(payload, indent=2, default=str))
