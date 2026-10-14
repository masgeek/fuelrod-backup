"""Backup verification: prove that a dump on disk is actually restorable."""

from __future__ import annotations

import gzip
import hashlib
import re
import shutil
import sys
import zipfile
import zlib
from dataclasses import dataclass, field
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .config import Config

console = Console()

# Artifact kinds, keyed by the extension chain after stripping optional .gz
_KIND_BY_EXT = {
    ".dump": "pg_custom",
    ".sql": "sql",
    ".zip": "zip",
    ".bak": "bak",
}

# Suffixes we recognise, most specific first (encrypted artifacts included).
_ARTIFACT_SUFFIXES = (
    "*.age", "*.gpg", "*.dump.gz", "*.dump", "*.sql.gz", "*.sql", "*.zip", "*.bak",
)

_STATUS_ORDER = {"fail": 0, "warn": 1, "skip": 2, "ok": 3}
_STATUS_ICON = {"ok": "[green]✓[/]", "warn": "[yellow]![/]", "fail": "[red]✗[/]", "skip": "[dim]-[/]"}


def _die(msg: str) -> None:
    console.print(f"[bold red]ERROR:[/] {msg}")
    sys.exit(1)


@dataclass
class Check:
    """One verification step applied to an artifact."""
    name: str
    status: str  # ok | warn | fail | skip
    detail: str = ""


@dataclass
class ArtifactResult:
    path: Path
    kind: str
    size: int
    checks: list[Check] = field(default_factory=list)

    @property
    def status(self) -> str:
        """Worst status across all checks."""
        return min((c.status for c in self.checks), key=lambda s: _STATUS_ORDER[s], default="skip")

    @property
    def ok(self) -> bool:
        return self.status in ("ok", "warn", "skip")

    def add(self, name: str, status: str, detail: str = "") -> None:
        self.checks.append(Check(name, status, detail))


# ──────────────────────────────────────────────────────────────────────────────
#  Artifact discovery
# ──────────────────────────────────────────────────────────────────────────────

def classify_artifact(path: Path) -> str:
    """Return the artifact kind for *path* (strips a trailing .gz)."""
    name = path.name
    for encrypted in (".age", ".gpg"):
        if name.endswith(encrypted):
            return f"encrypted:{encrypted.lstrip('.')}"
    if name.endswith(".gz"):
        name = name[: -len(".gz")]
    ext = Path(name).suffix.lower()
    return _KIND_BY_EXT.get(ext, "unknown")


def discover_artifacts(
    base_dir: Path,
    *,
    engine: str | None = None,
    project: str | None = None,
    database: str | None = None,
    latest_only: bool = False,
) -> list[Path]:
    """
    Find backup artifacts under *base_dir* in the canonical
    <project>/<engine>/<database>/ layout.

    Layout is not enforced — a flat scan is used as a fallback so ad-hoc
    directories are still verifiable.
    """
    if not base_dir.is_dir():
        _die(f"Backup directory not found: {base_dir}")

    found: list[Path] = []
    canonical = True

    for project_dir in sorted(p for p in base_dir.iterdir() if p.is_dir()):
        if project and project_dir.name != project:
            continue
        engine_dirs = [d for d in sorted(project_dir.iterdir()) if d.is_dir()]
        if not any(d.name in _KIND_BY_EXT.values() or d.name in ("postgres", "mariadb", "mssql")
                   for d in engine_dirs):
            canonical = False
            continue
        for engine_dir in engine_dirs:
            if engine and engine_dir.name != engine:
                continue
            for db_dir in sorted(d for d in engine_dir.iterdir() if d.is_dir()):
                if database and db_dir.name != database:
                    continue
                files = _files_in(db_dir)
                if latest_only and files:
                    files = [max(files, key=lambda p: p.stat().st_mtime)]
                found.extend(files)

    if not canonical:
        # Flat scan for directories that don't follow the layout.
        for pat in _ARTIFACT_SUFFIXES:
            found.extend(p for p in base_dir.rglob(pat) if p.is_file())

    return sorted(set(found), key=lambda p: str(p))


def _files_in(db_dir: Path) -> list[Path]:
    files: list[Path] = []
    for pat in _ARTIFACT_SUFFIXES:
        files.extend(p for p in db_dir.glob(pat) if p.is_file())
    return files


# ──────────────────────────────────────────────────────────────────────────────
#  Pure checks (no database, no network)
# ──────────────────────────────────────────────────────────────────────────────

def _check_readable(path: Path) -> Check:
    try:
        with path.open("rb") as fh:
            if not fh.read(1):
                return Check("readable", "fail", "file is empty")
    except OSError as exc:
        return Check("readable", "fail", str(exc))
    return Check("readable", "ok", f"{path.stat().st_size / 1024 / 1024:.2f} MB")


def _gzip_stream(path: Path):
    return gzip.open(path, "rb")


def _decompressed_size(path: Path) -> int:
    total = 0
    with _gzip_stream(path) as fh:
        while chunk := fh.read(1024 * 1024):
            total += len(chunk)
    return total


def check_sql(path: Path) -> list[Check]:
    """A plain-text SQL dump must decompress cleanly and look like SQL."""
    checks: list[Check] = [_check_readable(path)]
    try:
        if path.name.endswith(".gz"):
            plain = _decompressed_size(path)
            if plain == 0:
                checks.append(Check("gzip", "fail", "decompressed to 0 bytes"))
                return checks
            checks.append(Check("gzip", "ok", f"{plain / 1024 / 1024:.2f} MB uncompressed"))
            source = _gzip_stream(path)
        else:
            source = path.open("rb")

        with source as fh:
            head = fh.read(8192)
        checks.append(_looks_like_sql(head))
    except (OSError, EOFError, zlib.error) as exc:
        checks.append(Check("decompress", "fail", f"{type(exc).__name__}: {exc}"))
    return checks


def _looks_like_sql(head: bytes) -> Check:
    if not head:
        return Check("content", "fail", "no content")
    text = head.decode("utf-8", errors="replace").lstrip()
    if re.match(r"^(SET |SELECT |--|/\*|DROP |CREATE |USE |LOCK |DELIMITER )", text, re.IGNORECASE):
        return Check("content", "ok", "recognisable SQL header")
    return Check("content", "warn", "header does not look like SQL")


def check_zip(path: Path) -> list[Check]:
    checks: list[Check] = [_check_readable(path)]
    try:
        with zipfile.ZipFile(path) as zf:
            bad = zf.testzip()
            if bad is not None:
                checks.append(Check("crc", "fail", f"corrupt entry: {bad}"))
                return checks
            names = zf.namelist()
            if not names:
                checks.append(Check("crc", "fail", "archive is empty"))
                return checks
            checks.append(Check("crc", "ok", f"{len(names)} entries verified"))
    except (zipfile.BadZipFile, OSError) as exc:
        checks.append(Check("crc", "fail", f"BadZipFile: {exc}"))
    return checks


# A T-SQL backup set begins with the ASCII media header 'MICROSOFT'.
_BAK_MAGIC = b"MICROSOFT"


def check_bak(path: Path) -> list[Check]:
    checks: list[Check] = [_check_readable(path)]
    try:
        with path.open("rb") as fh:
            header = fh.read(len(_BAK_MAGIC))
    except OSError as exc:
        checks.append(Check("header", "fail", str(exc)))
        return checks

    if header == _BAK_MAGIC:
        checks.append(Check("header", "ok", "valid SQL Server backup header"))
    else:
        checks.append(Check(
            "header",
            "warn",
            "missing 'MICROSOFT' header — run RESTORE VERIFYONLY to confirm",
        ))
    return checks


def check_encrypted(path: Path, decrypt_tool: str | None = None) -> list[Check]:
    """Encrypted artifacts: confirm the envelope, and decrypt if a tool exists."""
    checks: list[Check] = [_check_readable(path)]
    try:
        head = path.read_bytes()[:16]
    except OSError as exc:
        checks.append(Check("header", "fail", str(exc)))
        return checks

    is_age = path.name.endswith(".age")
    expected = b"age-encryption.org/v1" if is_age else None
    if expected and not head.startswith(expected):
        checks.append(Check("header", "fail", "not a valid age header"))
        return checks
    checks.append(Check("header", "ok", f"encrypted ({'age' if is_age else 'gpg'}) envelope"))

    tool = decrypt_tool or ("age" if is_age else "gpg")
    if not shutil.which(tool):
        checks.append(Check("decrypt", "skip", f"'{tool}' not installed — not decrypted"))
        return checks
    checks.append(Check("decrypt", "skip", "pass --decrypt to test decryption"))
    return checks


def sha256_file(path: Path, chunk: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while block := fh.read(chunk):
            digest.update(block)
    return digest.hexdigest()


# ──────────────────────────────────────────────────────────────────────────────
#  PostgreSQL TOC check (needs pg_restore, never needs a live DB)
# ──────────────────────────────────────────────────────────────────────────────

def check_pg_custom(path: Path, cfg: Config | None) -> list[Check]:
    checks: list[Check] = [_check_readable(path)]
    if path.name.endswith(".gz"):
        try:
            plain = _decompressed_size(path)
            checks.append(Check("gzip", "ok" if plain else "fail", f"{plain / 1024 / 1024:.2f} MB uncompressed"))
        except (OSError, EOFError, zlib.error) as exc:
            checks.append(Check("gzip", "fail", f"{type(exc).__name__}: {exc}"))
            return checks

    if cfg is None:
        checks.append(Check("toc", "skip", "no engine config — TOC not checked"))
        return checks

    if not cfg.use_docker and not shutil.which(cfg.pg_restore_cmd):
        checks.append(Check("toc", "skip", f"{cfg.pg_restore_cmd} not on PATH"))
        return checks

    from .runner import PgRunner

    try:
        toc = PgRunner(cfg).read_toc(path)
    except FileNotFoundError:
        checks.append(Check("toc", "skip", f"{cfg.pg_restore_cmd} not found"))
        return checks
    except (OSError, ValueError) as exc:
        checks.append(Check("toc", "fail", f"unreadable dump: {exc}"))
        return checks

    entries = [line for line in toc.splitlines() if line and not line.startswith(";")]
    if not entries:
        checks.append(Check("toc", "fail", "dump contains no objects"))
        return checks
    checks.append(Check("toc", "ok", f"{len(entries)} object(s) listed"))
    return checks


# ──────────────────────────────────────────────────────────────────────────────
#  Orchestration
# ──────────────────────────────────────────────────────────────────────────────

def verify_artifact(path: Path, cfg: Config | None, *, checksum: bool = False) -> ArtifactResult:
    """Run every applicable check against one artifact."""
    kind = classify_artifact(path)
    result = ArtifactResult(path=path, kind=kind, size=path.stat().st_size)

    if kind == "pg_custom":
        result.checks += check_pg_custom(path, cfg)
    elif kind == "sql":
        result.checks += check_sql(path)
    elif kind == "zip":
        result.checks += check_zip(path)
    elif kind == "bak":
        result.checks += check_bak(path)
    elif kind.startswith("encrypted:"):
        result.checks += check_encrypted(path)
    else:
        result.add("kind", "warn", f"unknown artifact type: {path.name}")

    if checksum:
        try:
            result.add("sha256", "ok", sha256_file(path))
        except OSError as exc:
            result.add("sha256", "fail", str(exc))
    return result


def run_verify(
    cfg: Config,
    *,
    engine: str | None = None,
    project: str | None = None,
    database: str | None = None,
    latest_only: bool = False,
    checksum: bool = False,
) -> int:
    """Verify backup artifacts. Returns a process exit code."""
    base = Path(cfg.base_dir) if cfg.base_dir else Path(".")
    if not cfg.base_dir:
        _die("BASE_DIR is not set. Add it to .backup/.env config.")

    # The TOC check only needs pg_restore, and only for PostgreSQL artifacts.
    engine_cfg = cfg if cfg.db_type.value == (engine or cfg.db_type.value) else None

    artifacts = discover_artifacts(
        base,
        engine=engine,
        project=project,
        database=database,
        latest_only=latest_only,
    )
    if not artifacts:
        console.print(f"[yellow]No backup artifacts found under {base}.[/]")
        return 0

    console.print(Panel(f"[bold cyan]VERIFY[/] — {base}", expand=False))

    results = [verify_artifact(p, engine_cfg, checksum=checksum) for p in artifacts]

    table = Table(show_header=True, header_style="bold")
    table.add_column("", width=3)
    table.add_column("File", min_width=40, overflow="fold")
    table.add_column("Type", width=12)
    table.add_column("Size", justify="right", width=10)
    table.add_column("Checks", min_width=28, overflow="fold")
    for r in results:
        detail = "; ".join(f"{c.name}={c.status}" for c in r.checks)
        table.add_row(_STATUS_ICON[r.status], r.path.name, r.kind, _human(r.size), detail)
    console.print(table)

    failed = [r for r in results if not r.ok]
    warned = [r for r in results if r.status == "warn"]
    console.print()
    console.print(
        f"  [green]{len(results) - len(failed) - len(warned)} ok[/]  "
        f"[yellow]{len(warned)} warning(s)[/]  "
        f"[red]{len(failed)} failed[/]"
    )
    if failed:
        console.print()
        for r in failed:
            for c in r.checks:
                if c.status == "fail":
                    console.print(f"  [red]✗[/] {r.path.name} → {c.name}: {c.detail}")
        console.print()
    return 1 if failed else 0


def _human(size: int) -> str:
    if size > 1024 * 1024:
        return f"{size / 1024 / 1024:.1f} MB"
    if size > 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size} B"
