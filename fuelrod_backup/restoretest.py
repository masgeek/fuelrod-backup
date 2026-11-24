"""Restore drill: restore the newest backup of each database into a throwaway database and sanity-check it."""

from __future__ import annotations

import secrets
import sys
from pathlib import Path

from rich.console import Console

from .adapters import get_adapter
from .catalog import list_backups
from .config import Config
from .crypto import CryptoError, decrypt_if_needed
from .notify import EXIT_FAILURE, EXIT_OK, EXIT_VERIFY_FAILED
from .output import emit, error, json_enabled, register

console = register(Console())


def _newest_full(base_dir: str, engine: str, database: str | None) -> dict[str, Path]:
    """Newest non-incremental backup per database for *engine*."""
    latest: dict[str, Path] = {}
    for entry in list_backups(base_dir, [engine], database):
        if entry.incremental:
            continue
        latest[entry.database] = Path(entry.file)  # entries are oldest-first, so last wins
    return latest


def _restore_into(cfg: Config, adapter, dump: Path, scratch: str) -> None:
    if adapter.supports_toc:
        from .restore import _execute_pg_restore_v2

        _execute_pg_restore_v2(dump, scratch, ["--no-owner", "--no-privileges"], cfg)
    else:
        adapter.restore_db(scratch, dump, schemas=[], no_owner=True)


def run_restore_test(cfg: Config, *, database: str | None = None) -> int:
    """Return a process exit code: 0 all drills passed, 3 a drill failed, 1 operational error."""
    if not cfg.base_dir or not cfg.password:
        error("BASE_DIR and the database password must be configured for --restore-test.")
        return EXIT_FAILURE

    engine = cfg.db_type.value
    targets = _newest_full(cfg.base_dir, engine, database)
    if not targets:
        error(f"No full {engine} backups found under {cfg.base_dir}.")
        return EXIT_FAILURE

    adapter = get_adapter(cfg)
    results: list[dict] = []

    for db, dump in sorted(targets.items()):
        scratch = f"frb_restoretest_{secrets.token_hex(4)}"
        result = {"database": db, "file": str(dump), "scratch": scratch, "status": "fail", "detail": ""}
        working = dump
        created = False
        try:
            working = decrypt_if_needed(dump, cfg)
            adapter.create_db(scratch)
            created = True
            _restore_into(cfg, adapter, working, scratch)
            tables = adapter.get_table_count(scratch)
            if str(tables).strip().isdigit() and int(tables) == 0:
                result["detail"] = "restored but contains no tables"
            else:
                result.update(status="ok", detail=f"{tables} table(s)")
        except CryptoError as exc:
            result["detail"] = f"decrypt failed: {exc}"
        except Exception as exc:  # noqa: BLE001 — any failure is a failed drill, never a crash
            result["detail"] = str(exc)
        finally:
            if created:
                try:
                    adapter.drop_db(scratch)
                except Exception as exc:  # noqa: BLE001
                    result["detail"] += f" (cleanup of {scratch} failed: {exc})"
            if working != dump:
                working.unlink(missing_ok=True)
        results.append(result)
        if not json_enabled():
            icon = "[green]✓[/]" if result["status"] == "ok" else "[red]✗[/]"
            console.print(f"  {icon} {db}  [dim]{Path(result['file']).name}[/]  {result['detail']}")

    failed = [r for r in results if r["status"] != "ok"]
    if json_enabled():
        emit({"command": "verify", "mode": "restore-test", "engine": engine,
              "status": "fail" if failed else "ok", "results": results})
    elif failed:
        print(f"ERROR: {len(failed)} restore drill(s) failed", file=sys.stderr)
    return EXIT_VERIFY_FAILED if failed else EXIT_OK
