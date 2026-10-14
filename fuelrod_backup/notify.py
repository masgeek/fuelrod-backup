"""Run notifications: webhooks and command hooks.

Notifications are strictly best-effort — a failing webhook must never turn a
successful backup into a failed one. Send errors are reported on stderr and
swallowed.

Exit-code contract (shared by every command):

==== =========================================================================
0    success
1    operational failure (connection refused, dump failed, bad config)
2    usage error (missing/unknown option)
3    verification failed (`verify` found a broken artifact)
4    aborted by the operator (declined a confirmation, Ctrl+C)
==== =========================================================================
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
#  Exit codes
# ──────────────────────────────────────────────────────────────────────────────

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_VERIFY_FAILED = 3
EXIT_ABORTED = 4

# Webhook/hook failures are warnings, never a failed run.
_EVENTS = {"success", "failure"}


def exit_for(status: str) -> int:
    return EXIT_ABORTED if status == "aborted" else (EXIT_OK if status == "success" else EXIT_FAILURE)


# ──────────────────────────────────────────────────────────────────────────────
#  Notification
# ──────────────────────────────────────────────────────────────────────────────

def parse_events(raw: str) -> set[str]:
    """Parse NOTIFY_ON into a set of tokens (events plus the `all` wildcard)."""
    return {p.strip().lower() for p in raw.replace(",", " ").split() if p.strip()}


def wants(event: str, configured: str) -> bool:
    """True when *configured* (NOTIFY_ON) selects *event*."""
    tokens = parse_events(configured)
    if not tokens:
        return False
    if "all" in tokens:
        return True
    return event in tokens & _EVENTS


def _template_fields(status: str, command: str, payload: dict[str, Any]) -> dict[str, str]:
    fields = {
        "status": status,
        "command": command,
        "exit_code": str(payload.get("exit_code", EXIT_OK)),
        "summary": payload.get("summary_text", ""),
    }
    for key, value in payload.items():
        if key not in fields and isinstance(value, (str, int, float, bool)):
            fields.setdefault(str(key), str(value))
    return fields


def notify(
    *,
    status: str,
    command: str,
    cfg: Any,
    summary: str = "",
    exit_code: int | None = None,
    extra: dict[str, Any] | None = None,
) -> list[str]:
    """
    Fire the configured notifications for *status* ("success" | "failure").

    Returns the list of delivery errors (empty when everything worked).
    """
    errors: list[str] = []
    event = "success" if status == "success" else "failure"
    payload: dict[str, Any] = {
        "command": command,
        "status": status,
        "summary": summary,
        "exit_code": exit_code if exit_code is not None else exit_for(status),
        "engine": getattr(cfg, "db_type", None) and cfg.db_type.value,
        "base_dir": getattr(cfg, "base_dir", ""),
    }
    if extra:
        payload.update(extra)

    if not wants(event, getattr(cfg, "notify_on", "") or ""):
        return errors

    url = getattr(cfg, "notify_url", "") or ""
    if url:
        errors.extend(_send_webhook(url, payload, getattr(cfg, "notify_timeout", 15)))

    hook = ""
    if event == "success":
        hook = getattr(cfg, "notify_hook_success", "") or ""
    else:
        hook = getattr(cfg, "notify_hook_failure", "") or ""
    if hook:
        errors.extend(_run_hook(hook, payload, getattr(cfg, "notify_timeout", 15)))

    return errors


def _send_webhook(url: str, payload: dict[str, Any], timeout: int) -> list[str]:
    if not url.lower().startswith(("http://", "https://")):
        return [f"unsupported webhook scheme (only http/https): {url}"]

    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(  # noqa: S310 — scheme restricted to http/https above
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "fuelrod-backup"},
        method="POST",
    )
    try:
        # Scheme validated above; operator-supplied webhook endpoint.
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            if response.status >= 400:
                return [f"webhook {url} returned HTTP {response.status}"]
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return [f"webhook {url} failed: {exc}"]
    return []


def _run_hook(command: str, payload: dict[str, Any], timeout: int) -> list[str]:
    """
    Run a hook command with placeholders substituted.

    Placeholders are ``{status}``, ``{command}``, ``{exit_code}``, ``{summary}``
    plus any scalar payload key. The command runs with a minimal environment and
    a hard timeout — a hanging hook cannot block the run that triggered it.
    """
    fields = _template_fields(payload["status"], payload["command"], payload)
    try:
        rendered = command.format(**fields)
    except (KeyError, IndexError, ValueError) as exc:
        return [f"hook template error: {exc}"]

    # Minimal env: the hook gets no database credentials.
    passthrough = ("PATH", "HOME", "USERPROFILE", "APPDATA", "TEMP", "TMP")
    env = {key: os.environ[key] for key in passthrough if key in os.environ}
    env["FUELROD_STATUS"] = payload["status"]
    env["FUELROD_COMMAND"] = payload["command"]
    env["FUELROD_EXIT_CODE"] = str(payload["exit_code"])

    try:
        subprocess.run(  # noqa: S603 — rendered from the operator's own config, list form, no shell
            shlex.split(rendered, posix=os.name != "nt"),
            env=env,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return [f"hook timed out after {timeout}s: {rendered}"]
    except (OSError, ValueError) as exc:
        return [f"hook failed to start: {exc}"]
    return []


def report_delivery_errors(errors: list[str], console: Any) -> None:
    """Print delivery problems as warnings; never raise."""
    for err in errors:
        console.print(f"  [yellow]WARN:[/] notification: {err}", file=sys.stderr)
