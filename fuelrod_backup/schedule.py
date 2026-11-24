"""Schedule fuelrod-backup jobs (backup, pull, push, ...): cron on Linux/macOS, Task Scheduler on Windows.

Each job has its own entry, so a backup at 02:00, a push at 04:00 and a pull at 03:30 can all be
installed side by side, and re-installing one replaces only that one. Entries are tagged
(``# fuelrod-backup:<job>`` in cron, task ``fuelrod-backup-<job>`` on Windows); the original backup
entry (``# fuelrod-backup`` / task ``fuelrod-backup``) is still recognised as the ``backup`` job.
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
from dataclasses import dataclass

# job -> (default cron, command words, accepts --db-type)
JOBS: dict[str, tuple[str, list[str], bool]] = {
    "backup": ("0 2 * * *", ["backup", "--no-interactive"], True),
    "pull": ("30 3 * * *", ["pull"], False),
    "push": ("0 4 * * *", ["push", "--no-interactive"], False),
    "quota": ("0 5 * * *", ["quota"], False),
    "verify": ("30 5 * * *", ["verify", "--latest"], True),
    "doctor": ("0 6 * * 1", ["doctor"], True),
}

_CRON_MARK = "# fuelrod-backup"
_DAYS = {"0": "SUN", "7": "SUN", "1": "MON", "2": "TUE", "3": "WED", "4": "THU", "5": "FRI", "6": "SAT"}


def default_cron(job: str) -> str:
    return JOBS[job][0]


def task_name(job: str) -> str:
    return "fuelrod-backup" if job == "backup" else f"fuelrod-backup-{job}"


def cron_mark(job: str) -> str:
    return f"{_CRON_MARK}:{job}"


def _cron_ok(expr: str) -> bool:
    return bool(re.fullmatch(r"[\d*/,\-]+(\s+[\d*/,\-]+){4}", expr.strip()))


def build_command(
    job: str, db_type: str | None, config_file: str | None, extra_args: list[str] | None = None,
) -> list[str]:
    """argv for the scheduled run. *extra_args* (each may hold several words) is appended as given."""
    if job not in JOBS:
        raise ValueError(f"unknown job {job!r}; choose from: {', '.join(JOBS)}")
    _cron, words, accepts_db_type = JOBS[job]
    cmd = [shutil.which("fuelrod-backup") or "fuelrod-backup", *words, "--json"]
    if db_type and accepts_db_type:
        cmd += ["--db-type", db_type]
    if config_file:
        cmd += ["--config", config_file]
    for extra in extra_args or []:
        cmd += shlex.split(extra)
    return cmd


def default_log(job: str) -> str:
    """Absolute, so the job logs to the same place whatever folder cron / Task Scheduler starts it in."""
    from .config import user_config_dir

    return str(user_config_dir() / "logs" / f"fuelrod-backup-{job}.log")


# ───────────────────────── rendering ─────────────────────────

def _windows_schedule(cron: str) -> tuple[list[str], str]:
    """Map a cron expression to schtasks flags. Supports daily, every-N-hours and weekly; others fall back to daily."""
    minute, hour, dom, month, dow = cron.split()
    start = f"{int(hour) if hour.isdigit() else 2:02d}:{int(minute) if minute.isdigit() else 0:02d}"
    if minute.isdigit() and hour.startswith("*/") and hour[2:].isdigit() and (dom, month, dow) == ("*", "*", "*"):
        return ["/SC", "HOURLY", "/MO", hour[2:], "/ST", f"00:{int(minute):02d}"], ""
    if minute.isdigit() and hour.isdigit() and (dom, month) == ("*", "*"):
        if dow == "*":
            return ["/SC", "DAILY", "/ST", start], ""
        days = [_DAYS.get(d) for d in dow.split(",")]
        if all(days):
            return ["/SC", "WEEKLY", "/D", ",".join(days), "/ST", start], ""
    return ["/SC", "DAILY", "/ST", "02:00"], (
        "  (Windows supports daily, hourly (*/N) and weekly schedules here; defaulted to daily 02:00)\n"
    )


def render(
    job: str, cron: str, command: list[str], log_file: str, *, windows: bool | None = None,
) -> tuple[str, list[str] | None]:
    """Return (text to show or install, installer argv — only on Windows, where it is a schtasks command)."""
    if not _cron_ok(cron):
        raise ValueError(f"invalid cron expression {cron!r}; expected 5 fields, e.g. '0 2 * * *'")
    windows = (os.name == "nt") if windows is None else windows

    if windows:
        flags, note = _windows_schedule(cron)
        quoted_log = f'"{log_file}"' if " " in log_file else log_file
        line = subprocess.list2cmdline(command) + f" >> {quoted_log} 2>&1"
        argv = ["schtasks", "/Create", "/TN", task_name(job), *flags, "/TR", f"cmd /c {line}", "/F"]
        return note + subprocess.list2cmdline(argv), argv

    return f"{cron.strip()} {shlex.join(command)} >> {shlex.quote(log_file)} 2>&1 {cron_mark(job)}", None


# ───────────────────────── installing / listing / removing ─────────────────────────

@dataclass
class ScheduledJob:
    job: str
    when: str
    command: str


def _owned_by(line: str, job: str) -> bool:
    if cron_mark(job) in line:
        return True
    return job == "backup" and line.rstrip().endswith(_CRON_MARK)  # entry written by an older version


def _crontab() -> list[str]:
    current = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)  # noqa: S607
    return current.stdout.splitlines()


def _write_crontab(lines: list[str]) -> None:
    subprocess.run(["crontab", "-"], input="\n".join(lines) + "\n", text=True, check=True)  # noqa: S607


def install_cron(job: str, line: str) -> None:
    """Add *line* to the user's crontab, replacing that job's previous entry only."""
    _write_crontab([ln for ln in _crontab() if not _owned_by(ln, job)] + [line])


def remove_cron(job: str) -> bool:
    lines = _crontab()
    kept = [ln for ln in lines if not _owned_by(ln, job)]
    if len(kept) == len(lines):
        return False
    _write_crontab(kept)
    return True


def list_cron() -> list[ScheduledJob]:
    found = []
    for ln in _crontab():
        m = re.search(r"# fuelrod-backup(?::(\w+))?\s*$", ln)
        if not m:
            continue
        job = m.group(1) or "backup"
        fields = ln.split(None, 5)
        command = fields[5].rsplit("#", 1)[0].strip() if len(fields) > 5 else ""
        found.append(ScheduledJob(job, " ".join(fields[:5]), command))
    return found


def list_tasks() -> list[ScheduledJob]:
    """Windows: the fuelrod-backup tasks registered in Task Scheduler."""
    result = subprocess.run(  # noqa: S603, S607
        ["schtasks", "/Query", "/FO", "CSV", "/NH"], capture_output=True, text=True, check=False,
    )
    found = []
    for row in result.stdout.splitlines():
        cells = [c.strip('"') for c in row.split('","')]
        name = cells[0].lstrip("\\") if cells else ""
        if name == "fuelrod-backup" or name.startswith("fuelrod-backup-"):
            job = "backup" if name == "fuelrod-backup" else name[len("fuelrod-backup-"):]
            next_run = cells[1] if len(cells) > 1 else ""
            found.append(ScheduledJob(job, next_run, f"next run: {next_run}" if next_run else ""))
    return found


def delete_task(job: str) -> bool:
    result = subprocess.run(  # noqa: S603, S607
        ["schtasks", "/Delete", "/TN", task_name(job), "/F"], capture_output=True, text=True, check=False,
    )
    return result.returncode == 0


def list_jobs(*, windows: bool | None = None) -> list[ScheduledJob]:
    return list_tasks() if ((os.name == "nt") if windows is None else windows) else list_cron()


def remove_job(job: str, *, windows: bool | None = None) -> bool:
    return delete_task(job) if ((os.name == "nt") if windows is None else windows) else remove_cron(job)
