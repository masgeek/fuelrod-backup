"""Pre-flight diagnostics: check everything a run needs before it starts."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .config import Config
from .output import emit, json_enabled, register

console = register(Console())

_STATUS_ORDER = {"fail": 0, "warn": 1, "skip": 2, "ok": 3}
_STATUS_ICON = {"ok": "[green]✓[/]", "warn": "[yellow]![/]", "fail": "[red]✗[/]", "skip": "[dim]-[/]"}

# Free space below this is a warning — a dump that cannot land is worse than a warning.
_MIN_FREE_MB = 512


@dataclass
class Diagnosis:
    """One diagnostic result: what was checked, how it went, and what to do."""
    name: str
    status: str  # ok | warn | fail | skip
    detail: str = ""
    hint: str = ""

    @property
    def ok(self) -> bool:
        return self.status != "fail"


@dataclass
class Report:
    checks: list[Diagnosis] = field(default_factory=list)

    def add(self, name: str, status: str, detail: str = "", hint: str = "") -> Diagnosis:
        d = Diagnosis(name, status, detail, hint)
        self.checks.append(d)
        return d

    @property
    def failures(self) -> list[Diagnosis]:
        return [d for d in self.checks if d.status == "fail"]

    @property
    def warnings(self) -> list[Diagnosis]:
        return [d for d in self.checks if d.status == "warn"]

    def worst(self) -> str:
        return min(
            (d.status for d in self.checks),
            key=lambda s: _STATUS_ORDER[s],
            default="ok",
        )


# ──────────────────────────────────────────────────────────────────────────────
#  Individual checks
# ──────────────────────────────────────────────────────────────────────────────

_MIN_PYTHON = (3, 13)


def check_python(report: Report) -> None:
    version = ".".join(str(v) for v in sys.version_info[:3])
    if sys.version_info[:2] >= _MIN_PYTHON:
        report.add("Python runtime", "ok", f"{version}")
    else:
        report.add(
            "Python runtime", "fail", version,
            f"Python {_MIN_PYTHON[0]}.{_MIN_PYTHON[1]}+ is required (see pyproject requires-python).",
        )


def check_config_source(cfg: Config, report: Report) -> None:
    if cfg.config_source:
        report.add("Config file", "ok", str(cfg.config_source))
    else:
        report.add(
            "Config file", "fail", "none found",
            "Run `fuelrod-backup init` or pass --config /path/to/.backup",
        )


def check_credentials(cfg: Config, report: Report) -> None:
    missing = [k for k, v in (
        ("DB_USERNAME", cfg.user),
        ("DB_PASSWORD", cfg.password),
        ("DB_HOST", cfg.host),
        ("DB_PORT", cfg.port),
    ) if not v]
    if missing:
        report.add(
            "Credentials", "fail", f"missing: {', '.join(missing)}",
            "Set them in the config file or the environment.",
        )
    else:
        report.add("Credentials", "ok", f"{cfg.user}@{cfg.host}:{cfg.port}")


def check_base_dir(cfg: Config, report: Report) -> None:
    if not cfg.base_dir:
        report.add(
            "BASE_DIR", "fail", "not set",
            "Backups have nowhere to go — set BASE_DIR in the config file.",
        )
        return

    base = Path(cfg.base_dir)
    if not base.is_dir():
        report.add(
            "BASE_DIR", "fail", f"missing directory: {base}",
            "Create it, or point BASE_DIR somewhere that exists.",
        )
        return

    if not os.access(base, os.W_OK):
        report.add(
            "BASE_DIR", "fail", f"not writable: {base}",
            "Fix the permissions — backup/restore would fail.",
        )
        return

    report.add("BASE_DIR", "ok", str(base))

    try:
        free_mb = shutil.disk_usage(base).free / 1024 / 1024
    except OSError as exc:
        report.add("Disk space", "warn", f"unknown: {exc}")
        return

    if free_mb < _MIN_FREE_MB:
        report.add(
            "Disk space", "warn", f"only {free_mb:.0f} MB free",
            f"Backups need room; less than {_MIN_FREE_MB} MB left.",
        )
    else:
        report.add("Disk space", "ok", f"{free_mb:.0f} MB free")


def check_retention(cfg: Config, report: Report) -> None:
    if cfg.days_to_keep < 0:
        report.add(
            "Retention (KEEP_DAYS)", "fail", str(cfg.days_to_keep),
            "Use 0 to keep forever, or a positive number of days.",
        )
    elif cfg.days_to_keep == 0:
        report.add(
            "Retention (KEEP_DAYS)", "warn", "0 (keep forever)",
            "Backups will never be pruned — set a positive value to bound disk usage.",
        )
    else:
        report.add("Retention (KEEP_DAYS)", "ok", f"{cfg.days_to_keep} day(s)")


def check_docker(cfg: Config, report: Report) -> None:
    if not cfg.use_docker:
        report.add("Docker mode", "ok", "disabled — direct host connection")
        return

    if not shutil.which("docker"):
        report.add(
            "Docker", "fail", "docker not on PATH",
            "Install Docker, or set USE_DOCKER=false to connect directly.",
        )
        return

    try:
        state = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", cfg.service],
            capture_output=True,
            timeout=cfg.connection_timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        report.add("Docker", "fail", f"inspect failed: {exc}", f"Service '{cfg.service}'?")
        return

    status = state.stdout.decode(errors="replace").strip()
    if state.returncode != 0:
        report.add(
            "Docker", "fail", f"service '{cfg.service}' not found",
            f"Check the SERVICE setting — {status or state.stderr.decode(errors='replace').strip()}",
        )
    elif status != "running":
        report.add(
            "Docker", "warn", f"service '{cfg.service}' is {status}",
            "Start it before backing up.",
        )
    else:
        report.add("Docker", "ok", f"service '{cfg.service}' running")


def check_binaries(cfg: Config, report: Report) -> None:
    """Every external executable the configured engine needs."""
    required: dict[str, str] = {}
    optional: dict[str, str] = {}

    if cfg.db_type.value == "postgres":
        required = {"pg_dump": cfg.pg_dump_cmd, "pg_restore": cfg.pg_restore_cmd}
        optional = {"psql": cfg.psql_cmd}
    elif cfg.db_type.value == "mariadb":
        required = {"mariadb-dump": cfg.mysql_dump_cmd, "mysql": cfg.mysql_cmd}

    if not required and not optional:
        report.add("External tools", "skip", f"no host binaries for {cfg.db_type.value}")
        return

    if cfg.use_docker:
        report.add(
            "External tools", "ok",
            f"run inside container '{cfg.service}' — host binaries not needed",
        )
        return

    for label, binary in required.items():
        path = shutil.which(binary)
        if path:
            report.add(f"Binary: {label}", "ok", path)
        else:
            report.add(
                f"Binary: {label}", "fail", f"{binary} not on PATH",
                f"Install {label} or set the matching *_CMD config key.",
            )

    for label, binary in optional.items():
        path = shutil.which(binary)
        status = "ok" if path else "skip"
        report.add(f"Binary: {label}", status, path or f"{binary} not on PATH")


def check_connection(cfg: Config, report: Report) -> None:
    from .adapters import get_adapter

    try:
        get_adapter(cfg).check_connection()
    except Exception as exc:  # noqa: BLE001 — any driver error is a diagnosis, not a crash
        report.add(
            "Database connection", "fail", str(exc),
            "Run `fuelrod-backup test` for the full connection report.",
        )
    else:
        report.add("Database connection", "ok", f"{cfg.db_type.value} reachable")


def check_encryption(cfg: Config, report: Report) -> None:
    from .crypto import tool_available, validate_config

    if not cfg.encrypt:
        report.add(
            "Encryption at rest", "skip", "disabled (ENCRYPT=false)",
            "Backups are written unencrypted — set ENCRYPT=true with a recipient key.",
        )
        return

    if not tool_available(cfg.encrypt_tool):
        report.add(
            "Encryption at rest", "fail", f"{cfg.encrypt_tool} not on PATH",
            f"Install {cfg.encrypt_tool}, or set ENCRYPT_TOOL, or set ENCRYPT=false.",
        )
        return

    try:
        validate_config(cfg)
    except Exception as exc:  # noqa: BLE001 — CryptoError carries the user-facing message
        report.add("Encryption at rest", "fail", str(exc))
        return

    report.add("Encryption at rest", "ok", f"{cfg.encrypt_tool} → {cfg.encrypt_recipient or 'symmetric'}")
    if cfg.encrypt_tool == "age" and cfg.encrypt_identity_file:
        if not Path(cfg.encrypt_identity_file).is_file():
            report.add(
                "Encryption identity file", "fail",
                f"missing: {cfg.encrypt_identity_file}",
                "Restores of encrypted backups need this private key file.",
            )
        else:
            report.add("Encryption identity file", "ok", cfg.encrypt_identity_file)


def check_rclone(cfg: Config, report: Report) -> None:
    if not shutil.which("rclone"):
        report.add(
            "rclone (gdrive-sync)", "skip", "not installed",
            "Only needed for `gdrive-sync` — install from https://rclone.org/install/",
        )
        return

    try:
        remotes = subprocess.run(
            ["rclone", "listremotes"], capture_output=True, timeout=15, check=False
        )
        names = {
            line.split(":", 1)[0]
            for line in remotes.stdout.decode(errors="replace").splitlines()
            if line.strip()
        }
    except (OSError, subprocess.TimeoutExpired) as exc:
        report.add("rclone (gdrive-sync)", "warn", f"listremotes failed: {exc}")
        return

    wanted = cfg.gdrive_remote.split(":", 1)[0]
    if not wanted:
        report.add("rclone (gdrive-sync)", "warn", "GDRIVE is empty", "Set GDRIVE to a remote name.")
    elif wanted in names:
        report.add("rclone (gdrive-sync)", "ok", f"remote '{wanted}' configured")
    else:
        report.add(
            "rclone (gdrive-sync)", "warn", f"remote '{wanted}' not configured",
            "Run `rclone config` — gdrive-sync will fail until it exists.",
        )


def check_n8n_services(cfg: Config, report: Report) -> None:
    if not cfg.n8n_services:
        report.add("n8n services", "skip", "N8N_SERVICES is empty")
        return
    report.add("n8n services", "ok", ", ".join(cfg.n8n_services))

    unknown = [s for s in cfg.skip_services if s not in cfg.n8n_services]
    if unknown:
        report.add(
            "n8n skip list", "warn", f"not in N8N_SERVICES: {', '.join(unknown)}",
            "SKIP_SERVICES entries must match a configured service.",
        )


# ──────────────────────────────────────────────────────────────────────────────
#  Orchestration
# ──────────────────────────────────────────────────────────────────────────────

def run_doctor(cfg: Config, *, skip_connection: bool = False) -> int:
    """Run every diagnostic. Returns a process exit code."""
    report = Report()

    check_python(report)
    check_config_source(cfg, report)
    check_credentials(cfg, report)
    check_base_dir(cfg, report)
    check_retention(cfg, report)
    check_binaries(cfg, report)
    check_docker(cfg, report)
    if not skip_connection:
        check_connection(cfg, report)
    check_encryption(cfg, report)
    check_rclone(cfg, report)
    check_n8n_services(cfg, report)

    if json_enabled():
        emit({
            "command": "doctor",
            "status": "fail" if report.failures else "ok",
            "summary": {
                "total": len(report.checks),
                "ok": len(report.checks) - len(report.failures) - len(report.warnings),
                "warn": len(report.warnings),
                "fail": len(report.failures),
                "skip": sum(1 for d in report.checks if d.status == "skip"),
            },
            "checks": [
                {"name": d.name, "status": d.status, "detail": d.detail, "hint": d.hint}
                for d in report.checks
            ],
        })
        return 1 if report.failures else 0

    console.print(Panel("[bold cyan]DOCTOR[/] — pre-flight checks", expand=False))

    table = Table(show_header=True, header_style="bold")
    table.add_column("", width=3)
    table.add_column("Check", min_width=22)
    table.add_column("Status", min_width=10, overflow="fold")
    table.add_column("Detail", min_width=30, overflow="fold")
    for d in report.checks:
        table.add_row(_STATUS_ICON[d.status], d.name, d.status, d.detail)
    console.print(table)

    for d in report.failures + report.warnings:
        if d.hint:
            console.print(f"  {_STATUS_ICON[d.status]} [bold]{d.name}:[/] {d.hint}")

    console.print()
    ok = len(report.checks) - len(report.failures) - len(report.warnings)
    console.print(
        f"  [green]{ok} ok[/]  [yellow]{len(report.warnings)} warning(s)[/]  "
        f"[red]{len(report.failures)} failed[/]"
    )
    console.print()
    return 1 if report.failures else 0
