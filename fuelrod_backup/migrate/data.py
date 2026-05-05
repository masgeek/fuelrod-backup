"""Data migration: streaming rows from MariaDB into PostgreSQL in batches."""

from __future__ import annotations

import json
import time
from pathlib import Path

from .report import TableResult
from .schema import ColumnDef

_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 2.0
_LARGE_TABLE_THRESHOLD = 1_000_000

# Types that need Python-side coercion before PostgreSQL insertion
_NEEDS_COERCION = frozenset({
    "json", "tinyint", "bit",
    "tinyblob", "blob", "mediumblob", "longblob", "binary", "varbinary",
})


class DataMigrator:
    """Migrates a single table from MariaDB to PostgreSQL using server-side cursors."""

    def migrate_table(
        self,
        table: str,
        columns: list[ColumnDef],
        src_cfg,
        dst_runner,
        src_db: str,
        dst_db: str,
        target_schema: str = "public",
        batch_size: int = 1000,
        skipped_log: Path | None = None,
        progress=None,   # rich.progress.Progress | None
        overall_task=None,  # TaskID for the overall tables counter
    ) -> TableResult:
        import pymysql
        import pymysql.cursors

        result = TableResult(table=table)
        t_start = time.monotonic()

        # Exclude AUTO_INCREMENT (identity) columns — PostgreSQL assigns those automatically.
        identity_names = {c.name for c in columns if "auto_increment" in c.extra.lower()}
        insert_columns = [c for c in columns if c.name not in identity_names]
        col_names = [c.name for c in insert_columns]
        # Index positions of non-identity columns in the full source row
        insert_indexes = [i for i, c in enumerate(columns) if c.name not in identity_names]

        needs_coerce = any(c.data_type.lower() in _NEEDS_COERCION for c in insert_columns)

        # Reserve a progress row immediately so the table appears in the display
        task_id = None
        if progress is not None:
            task_id = progress.add_task(f"[dim]{table}[/]", total=None)

        # Count source rows
        try:
            src_count_raw = self._src_count(table, src_db, src_cfg)
            result.rows_source = src_count_raw
            if task_id is not None:
                progress.update(task_id, total=max(src_count_raw, 1))
        except Exception as exc:
            result.status = "failed"
            result.error = f"source count failed: {exc}"
            result.duration_s = time.monotonic() - t_start
            if task_id is not None:
                progress.update(task_id, description=f"[red]✗ {table}[/]", total=1, completed=1)
            return result

        # Open source connection with REPEATABLE READ snapshot
        try:
            src_conn = self._open_src(src_cfg, src_db)
        except Exception as exc:
            result.status = "failed"
            result.error = f"source connection failed: {exc}"
            result.duration_s = time.monotonic() - t_start
            if task_id is not None:
                progress.update(task_id, description=f"[red]✗ {table}[/]", completed=1)
            return result

        skipped_rows: list[dict] = []
        inserted = 0

        # SELECT all columns (we need the full row for coercion context),
        # but only INSERT the non-identity ones.
        all_col_names = [c.name for c in columns]
        try:
            with src_conn.cursor(pymysql.cursors.SSCursor) as cur:
                cur.execute(
                    f"SELECT {', '.join('`' + c + '`' for c in all_col_names)} "  # noqa: S608
                    f"FROM `{table}`"
                )
                if task_id is not None:
                    progress.update(task_id, description=f"[cyan]{table}[/]")
                batch: list[tuple] = []
                while True:
                    row = cur.fetchone()
                    if row is None:
                        if batch:
                            ok, skip = self._flush_batch(
                                batch, table, col_names, insert_columns,
                                insert_indexes, dst_runner, dst_db, target_schema,
                                needs_coerce, skipped_log,
                            )
                            inserted += ok
                            skipped_rows.extend(skip)
                            if task_id is not None:
                                progress.advance(task_id, ok)
                        break
                    batch.append(row)
                    if len(batch) >= batch_size:
                        ok, skip = self._flush_batch(
                            batch, table, col_names, insert_columns,
                            insert_indexes, dst_runner, dst_db, target_schema,
                            needs_coerce, skipped_log,
                        )
                        inserted += ok
                        skipped_rows.extend(skip)
                        if task_id is not None:
                            progress.advance(task_id, ok)
                        batch = []
        except Exception as exc:
            result.status = "failed"
            result.error = str(exc)
        finally:
            src_conn.close()

        result.rows_dest = inserted
        result.rows_skipped = len(skipped_rows)
        result.duration_s = time.monotonic() - t_start

        if result.status != "failed":
            result.status = "partial" if skipped_rows else "ok"

        if task_id is not None:
            if result.status == "ok":
                progress.update(task_id, description=f"[green]✓ {table}[/]", completed=inserted)
            elif result.status == "partial":
                skipped = len(skipped_rows)
                progress.update(task_id, description=f"[yellow]⚠ {table} ({skipped} skipped)[/]", completed=inserted)
            else:
                progress.update(task_id, description=f"[red]✗ {table}[/]", completed=max(inserted, 1))

        return result

    # ──────────────────────────────────────────────────────────────────────────

    def _open_src(self, src_cfg, dbname: str):
        import pymysql
        conn = pymysql.connect(
            host=src_cfg.host,
            port=src_cfg.port,
            user=src_cfg.user,
            password=src_cfg.password,
            database=dbname,
            charset="utf8mb4",
            connect_timeout=src_cfg.connection_timeout,
        )
        with conn.cursor() as cur:
            cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        return conn

    def _src_count(self, table: str, dbname: str, src_cfg) -> int:
        import pymysql
        conn = pymysql.connect(
            host=src_cfg.host,
            port=src_cfg.port,
            user=src_cfg.user,
            password=src_cfg.password,
            database=dbname,
            charset="utf8mb4",
            connect_timeout=src_cfg.connection_timeout,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{table}`")  # noqa: S608
                row = cur.fetchone()
                return int(row[0]) if row else 0
        finally:
            conn.close()

    def _flush_batch(
        self,
        batch: list[tuple],
        table: str,
        col_names: list[str],
        columns: list[ColumnDef],
        insert_indexes: list[int],
        pg_runner,
        dst_db: str,
        target_schema: str,
        needs_coerce: bool,
        skipped_log: Path | None,
    ) -> tuple[int, list[dict]]:
        skipped: list[dict] = []
        ok_rows: list[tuple] = []

        for row in batch:
            try:
                projected = tuple(row[i] for i in insert_indexes)
                coerced = self._coerce_row(projected, columns) if needs_coerce else projected
                ok_rows.append(coerced)
            except Exception as exc:
                skipped.append({"table": table, "row": list(map(repr, row)), "error": str(exc)})

        if not ok_rows:
            self._log_skipped(skipped, skipped_log)
            return 0, skipped

        inserted = 0
        for attempt in range(_MAX_RETRIES):
            try:
                inserted = self._insert_batch(ok_rows, table, col_names, pg_runner, dst_db, target_schema)
                break
            except Exception as exc:
                if attempt == _MAX_RETRIES - 1:
                    for row in ok_rows:
                        skipped.append({"table": table, "row": list(map(repr, row)), "error": str(exc)})
                    self._log_skipped(skipped, skipped_log)
                    return 0, skipped
                time.sleep(_RETRY_BACKOFF_BASE ** attempt)

        self._log_skipped(skipped, skipped_log)
        return inserted, skipped

    def _insert_batch(
        self,
        rows: list[tuple],
        table: str,
        col_names: list[str],
        pg_runner,
        dst_db: str,
        target_schema: str,
    ) -> int:
        col_list = ", ".join(f'"{c}"' for c in col_names)
        placeholders = ", ".join(["%s"] * len(col_names))
        sql = (
            f'INSERT INTO "{target_schema}"."{table}" ({col_list}) '  # noqa: S608
            f'VALUES ({placeholders})'
        )
        # Use individual execute() calls inside one transaction rather than
        # executemany(). psycopg3's executemany() uses pipeline mode internally;
        # when any row fails the whole pipeline aborts with an opaque
        # "pipeline aborted" message instead of a clean per-row error.
        with pg_runner._connect(dst_db) as conn:
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, row)
            conn.commit()
        return len(rows)

    @staticmethod
    def _coerce_row(row: tuple, columns: list[ColumnDef]) -> tuple:
        result = []
        for value, col in zip(row, columns):
            if value is None:
                result.append(None)
                continue
            dt = col.data_type.lower()
            if dt == "tinyint" and "tinyint(1)" in col.column_type.lower():
                result.append(bool(value))
            elif dt == "json":
                if isinstance(value, str):
                    parsed = json.loads(value)
                    result.append(json.dumps(parsed))
                else:
                    result.append(json.dumps(value))
            elif dt == "bit":
                # PyMySQL returns BIT(n) as bytes; store as BOOLEAN
                raw = bytes(value) if not isinstance(value, bytes) else value
                result.append(bool(int.from_bytes(raw, "big")))
            elif dt in ("tinyblob", "blob", "mediumblob", "longblob", "binary", "varbinary"):
                result.append(bytes(value) if not isinstance(value, bytes) else value)
            else:
                result.append(value)
        return tuple(result)

    @staticmethod
    def _log_skipped(skipped: list[dict], log_path: Path | None) -> None:
        if not skipped or log_path is None:
            return
        with log_path.open("a", encoding="utf-8") as f:
            for entry in skipped:
                f.write(json.dumps(entry) + "\n")
