"""Migration report: per-table status tracking and summary output."""

from __future__ import annotations

import json
import threading
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal

from rich.console import Console
from rich.table import Table


@dataclass
class TableResult:
    table: str
    status: Literal["ok", "failed", "skipped", "partial"] = "ok"
    rows_source: int = 0
    rows_dest: int = 0
    rows_skipped: int = 0
    count_match: bool = False
    checksum_match: bool | None = None
    duration_s: float = 0.0
    warnings: list[str] = field(default_factory=list)
    error: str | None = None


class MigrationReport:
    """Thread-safe collector of per-table migration results."""

    def __init__(self) -> None:
        self._results: list[TableResult] = []
        self._lock = threading.Lock()

    def record(self, result: TableResult) -> None:
        with self._lock:
            self._results.append(result)

    @property
    def results(self) -> list[TableResult]:
        with self._lock:
            return list(self._results)

    def has_failures(self) -> bool:
        return any(r.status == "failed" for r in self.results)

    def print_summary(self, console: Console) -> None:
        results = self.results
        if not results:
            console.print("[dim]No tables migrated.[/]")
            return

        t = Table(show_header=True, header_style="bold")
        t.add_column("Table", min_width=20)
        t.add_column("Status", width=8)
        t.add_column("Src Rows", justify="right")
        t.add_column("Dst Rows", justify="right")
        t.add_column("Match", width=5)
        t.add_column("Checksum", width=8)
        t.add_column("Time", justify="right", width=7)
        t.add_column("Warnings", min_width=10)

        for r in sorted(results, key=lambda x: x.table):
            if r.status == "ok":
                status_str = "[green]ok[/]"
            elif r.status == "partial":
                status_str = "[yellow]partial[/]"
            elif r.status == "skipped":
                status_str = "[dim]skip[/]"
            else:
                status_str = "[red]failed[/]"

            match_str = "[green]✓[/]" if r.count_match else "[red]✗[/]"
            if r.checksum_match is None:
                cksum_str = "[dim]—[/]"
            elif r.checksum_match:
                cksum_str = "[green]✓[/]"
            else:
                cksum_str = "[red]✗[/]"

            warn_str = f"[yellow]{len(r.warnings)}[/]" if r.warnings else "[dim]0[/]"
            t.add_row(
                r.table,
                status_str,
                f"{r.rows_source:,}",
                f"{r.rows_dest:,}",
                match_str,
                cksum_str,
                f"{r.duration_s:.1f}s",
                warn_str,
            )

        console.print(t)

        total = len(results)
        ok = sum(1 for r in results if r.status == "ok")
        failed = sum(1 for r in results if r.status == "failed")
        partial = sum(1 for r in results if r.status == "partial")
        skipped = sum(1 for r in results if r.status == "skipped")
        total_rows = sum(r.rows_dest for r in results)
        total_skipped = sum(r.rows_skipped for r in results)

        console.print(
            f"  Tables: {total} total — "
            f"[green]{ok} ok[/], [red]{failed} failed[/], "
            f"[yellow]{partial} partial[/], [dim]{skipped} skipped[/]"
        )
        console.print(f"  Rows migrated : {total_rows:,}")
        if total_skipped:
            console.print(f"  Rows skipped  : [yellow]{total_skipped:,}[/]")

        if failed or partial:
            console.print()
            for r in results:
                if r.status in ("failed", "partial") and r.error:
                    console.print(f"  [red]{r.table}:[/] {r.error}")
            for r in results:
                if r.warnings:
                    for w in r.warnings:
                        console.print(f"  [yellow]warn [{r.table}]:[/] {w}")

    def write_json(self, path: Path) -> None:
        data = {
            "tables": [asdict(r) for r in self.results],
            "summary": {
                "total": len(self._results),
                "ok": sum(1 for r in self._results if r.status == "ok"),
                "failed": sum(1 for r in self._results if r.status == "failed"),
                "partial": sum(1 for r in self._results if r.status == "partial"),
                "skipped": sum(1 for r in self._results if r.status == "skipped"),
                "total_rows_migrated": sum(r.rows_dest for r in self._results),
                "total_rows_skipped": sum(r.rows_skipped for r in self._results),
            },
        }
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
