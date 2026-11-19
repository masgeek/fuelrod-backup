"""Generate (and optionally install) a scheduled backup job: cron on POSIX, Task Scheduler on Windows."""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess

_TASK_NAME = "fuelrod-backup"
_CRON_MARK = "# fuelrod-backup"


def _cron_ok(expr: str) -> bool:
    return bool(re.fullmatch(r"[\d*/,\-]+(\s+[\d*/,\-]+){4}", expr.strip()))


def _daily_time(expr: str) -> str | None:
    m = re.fullmatch(r"(\d{1,2})\s+(\d{1,2})\s+\*\s+\*\s+\*", expr.strip())
    return f"{int(m.group(2)):02d}:{int(m.group(1)):02d}" if m else None


def build_command(db_type: str | None, config_file: str | None) -> list[str]:
    cmd = [shutil.which("fuelrod-backup") or "fuelrod-backup"]
    cmd += ["backup", "--no-interactive", "--json"]
    if db_type:
        cmd += ["--db-type", db_type]
    if config_file:
        cmd += ["--config", config_file]
    return cmd


def render(cron: str, command: list[str], log_file: str) -> tuple[str, list[str] | None]:
    """Return (human description, installer argv or None when it can't be installed automatically)."""
    if not _cron_ok(cron):
        raise ValueError(f"invalid cron expression {cron!r}; expected 5 fields, e.g. '0 2 * * *'")

    if os.name == "nt":
        at = _daily_time(cron)
        if " " in log_file:
            raise ValueError("--log must not contain spaces on Windows")
        line = subprocess.list2cmdline(command) + f" >> {log_file} 2>&1"
        text = f"schtasks /Create /TN {_TASK_NAME} /SC DAILY /ST {at or '02:00'} /TR \"cmd /c {line}\" /F"
        note = "" if at else "  (only daily schedules are generated on Windows; defaulted to 02:00)\n"
        argv = ["schtasks", "/Create", "/TN", _TASK_NAME, "/SC", "DAILY", "/ST", at or "02:00",
                "/TR", f"cmd /c {line}", "/F"]
        return note + text, argv

    line = f"{cron.strip()} {shlex.join(command)} >> {shlex.quote(log_file)} 2>&1 {_CRON_MARK}"
    return line, None


def install_cron(line: str) -> None:
    """Append *line* to the user's crontab, replacing any previous fuelrod-backup entry."""
    current = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)  # noqa: S607
    kept = [ln for ln in current.stdout.splitlines() if _CRON_MARK not in ln]
    new = "\n".join([*kept, line]) + "\n"
    subprocess.run(["crontab", "-"], input=new, text=True, check=True)  # noqa: S607
