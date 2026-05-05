"""MigrateRunner — top-level migration pipeline for one database."""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from ..adapters.mariadb import MariaDbAdapter
from ..runner import PgRunner
from .data import DataMigrator, _LARGE_TABLE_THRESHOLD
from .report import MigrationReport, TableResult
from .schema import DatabaseSchema, GeneratedDDL, SchemaExtractor, SchemaGenerator
from .validate import ValidationResult, Validator

console = Console()


class MigrateError(RuntimeError):
    pass


class MigrateRunner:
    def __init__(
        self,
        src_cfg,
        dst_cfg,
        *,
        target_schema: str = "public",
        batch_size: int = 1000,
        parallel: int = 4,
        dry_run: bool = False,
        validate: bool = True,
        validate_checksums: bool = False,
        fail_fast: bool = False,
        unsigned_checks: bool = False,
        enum_as_type: bool = False,
        skip_tables: list[str] | None = None,
        only_tables: list[str] | None = None,
        report_file: Path | None = None,
    ) -> None:
        self._src_cfg = src_cfg
        self._dst_cfg = dst_cfg
        self._target_schema = target_schema
        self._batch_size = batch_size
        self._parallel = parallel
        self._dry_run = dry_run
        self._validate = validate
        self._validate_checksums = validate_checksums
        self._fail_fast = fail_fast
        self._unsigned_checks = unsigned_checks
        self._enum_as_type = enum_as_type
        self._skip_tables: set[str] = set(skip_tables or [])
        self._only_tables: set[str] = set(only_tables or [])
        self._report_file = report_file

        self._src_adapter = MariaDbAdapter(src_cfg)
        self._dst_runner = PgRunner(dst_cfg)
        self._report = MigrationReport()
        self._migrator = DataMigrator()
        self._validator = Validator()

    def migrate_database(self, src_db: str, dst_db: str) -> MigrationReport:
        console.rule(f"[bold cyan]Migrating '{src_db}' → '{dst_db}'[/]")

        # ── 1. Extract schema ─────────────────────────────────────────────────
        console.print("  Extracting schema from MariaDB…")
        extractor = SchemaExtractor(self._src_adapter)
        schema = extractor.extract(src_db)
        console.print(
            f"  Found {len(schema.tables)} tables, {len(schema.views)} views, "
            f"{len(schema.triggers)} triggers, {len(schema.routines)} routines."
        )

        # ── 2. Filter tables ──────────────────────────────────────────────────
        tables = schema.tables
        if self._only_tables:
            tables = [t for t in tables if t.name in self._only_tables]
        if self._skip_tables:
            tables = [t for t in tables if t.name not in self._skip_tables]

        # ── 3. Generate DDL ───────────────────────────────────────────────────
        console.print("  Generating PostgreSQL DDL…")
        generator = SchemaGenerator(
            target_schema=self._target_schema,
            unsigned_checks=self._unsigned_checks,
            enum_as_type=self._enum_as_type,
        )
        schema_for_gen = DatabaseSchema(
            name=schema.name,
            tables=tables,
            views=schema.views,
            triggers=schema.triggers,
            routines=schema.routines,
            charset=schema.charset,
            collation=schema.collation,
        )
        generated: GeneratedDDL = generator.generate(schema_for_gen)

        # Write sidecar files for unsupported objects
        self._write_sidecars(schema, dst_db)

        if self._dry_run:
            console.print()
            console.print("[bold yellow]DRY RUN — no writes.[/] Generated DDL preview:\n")
            for stmt in generated.pre_data[:5]:
                console.print(f"[dim]{stmt[:200]}…[/]" if len(stmt) > 200 else f"[dim]{stmt}[/]")
            if len(generated.pre_data) > 5:
                console.print(f"[dim]… and {len(generated.pre_data) - 5} more tables.[/]")
            for w in generated.warnings:
                console.print(f"  [yellow]warn:[/] {w}")
            return self._report

        # ── 4. Ensure target DB and schema exist ──────────────────────────────
        self._ensure_target_db(dst_db)

        # ── 5. Apply pre-data DDL (CREATE TABLE) ──────────────────────────────
        console.print("  Applying schema to PostgreSQL…")
        for stmt in generated.pre_data:
            try:
                self._dst_runner._execute(stmt, dbname=dst_db)
            except Exception as exc:
                raise MigrateError(f"DDL failed: {exc}\nStatement: {stmt[:200]}") from exc

        # ── 6. Migrate data ───────────────────────────────────────────────────
        console.print(f"  Migrating {len(tables)} tables (parallel={self._parallel})…")

        skipped_log = Path(f"{dst_db}_skipped_rows.json")

        small = [t for t in tables if (t.auto_increment_start or 0) < _LARGE_TABLE_THRESHOLD]
        large = [t for t in tables if (t.auto_increment_start or 0) >= _LARGE_TABLE_THRESHOLD]

        failed = False
        tables_done = 0
        n_tables = len(tables)

        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}", min_width=35),
            BarColumn(bar_width=36),
            MofNCompleteColumn(),
            TextColumn("[dim]rows[/]"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            overall = progress.add_task(
                f"[bold cyan]tables 0/{n_tables}[/]",
                total=n_tables,
            )

            def _do_table(tdef) -> TableResult:
                return self._migrator.migrate_table(
                    table=tdef.name,
                    columns=tdef.columns,
                    src_cfg=self._src_cfg,
                    dst_runner=self._dst_runner,
                    src_db=src_db,
                    dst_db=dst_db,
                    target_schema=self._target_schema,
                    batch_size=self._batch_size,
                    skipped_log=skipped_log,
                    progress=progress,
                )

            # Small tables: parallel
            with ThreadPoolExecutor(max_workers=self._parallel) as pool:
                futures = {pool.submit(_do_table, tdef): tdef for tdef in small}
                for fut in as_completed(futures):
                    res = fut.result()
                    self._report.record(res)
                    tables_done += 1
                    progress.update(
                        overall,
                        advance=1,
                        description=f"[bold cyan]tables {tables_done}/{n_tables}[/]",
                    )
                    if res.status == "failed":
                        failed = True
                        if self._fail_fast:
                            pool.shutdown(wait=False, cancel_futures=True)
                            break

            # Large tables: sequential in main thread
            for tdef in large:
                if failed and self._fail_fast:
                    break
                res = _do_table(tdef)
                self._report.record(res)
                tables_done += 1
                progress.update(
                    overall,
                    advance=1,
                    description=f"[bold cyan]tables {tables_done}/{n_tables}[/]",
                )
                if res.status == "failed":
                    failed = True
                    if self._fail_fast:
                        break

        # ── 7. Apply post-data DDL (indexes, FKs, comments, sequence resets) ──
        if not (failed and self._fail_fast):
            console.print("  Applying post-data DDL (indexes, constraints, sequences)…")
            for stmt in generated.post_data:
                if stmt.startswith("--"):
                    continue
                try:
                    self._dst_runner._execute(stmt, dbname=dst_db)
                except Exception as exc:
                    console.print(f"  [yellow]warn:[/] post-DDL failed (non-fatal): {exc}")

        # ── 8. Validate ───────────────────────────────────────────────────────
        if self._validate and not (failed and self._fail_fast):
            console.print("  Validating row counts…")
            for tdef in tables:
                pk_cols = [c.name for c in tdef.columns if c.key == "PRI"]
                col_names = [c.name for c in tdef.columns]
                col_types = [c.data_type for c in tdef.columns]
                vr: ValidationResult = self._validator.validate_table(
                    table=tdef.name,
                    src_adapter=self._src_adapter,
                    pg_runner=self._dst_runner,
                    src_db=src_db,
                    dst_db=dst_db,
                    target_schema=self._target_schema,
                    pk_cols=pk_cols,
                    column_names=col_names,
                    column_types=col_types,
                    checksums=self._validate_checksums,
                )
                # Merge validation result back into existing TableResult
                for existing in self._report.results:
                    if existing.table == tdef.name:
                        existing.count_match = vr.count_match
                        existing.checksum_match = vr.checksum_match
                        if vr.errors:
                            existing.warnings.extend(vr.errors)
                        break

        # ── 9. Write report ───────────────────────────────────────────────────
        if self._report_file:
            self._report.write_json(self._report_file)
            console.print(f"  Report written: [bold]{self._report_file}[/]")

        if generated.warnings:
            console.print()
            console.print(f"  [yellow]{len(generated.warnings)} schema warning(s):[/]")
            for w in generated.warnings[:10]:
                console.print(f"    • {w}")
            if len(generated.warnings) > 10:
                console.print(f"    … and {len(generated.warnings) - 10} more (see report)")

        return self._report

    def _ensure_target_db(self, dst_db: str) -> None:
        import psycopg

        # Check/create the database via a direct autocommit connection to 'postgres'
        try:
            conn = psycopg.connect(
                host=self._dst_cfg.host,
                port=self._dst_cfg.port,
                dbname="postgres",
                user=self._dst_cfg.user,
                password=self._dst_cfg.password,
                connect_timeout=self._dst_cfg.connection_timeout,
                autocommit=True,
            )
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dst_db,))
                if not cur.fetchone():
                    console.print(f"  Creating database '{dst_db}' on PostgreSQL…")
                    cur.execute(psycopg.sql.SQL("CREATE DATABASE {}").format(
                        psycopg.sql.Identifier(dst_db)
                    ))
            conn.close()
        except Exception as exc:
            raise MigrateError(f"Cannot create target database '{dst_db}': {exc}") from exc

        # Ensure target schema exists
        try:
            self._dst_runner._execute(
                f'CREATE SCHEMA IF NOT EXISTS "{self._target_schema}"',
                dbname=dst_db,
            )
        except Exception as exc:
            raise MigrateError(f"Cannot create schema '{self._target_schema}': {exc}") from exc

    def _write_sidecars(self, schema: DatabaseSchema, dst_db: str) -> None:
        if schema.views:
            p = Path(f"{dst_db}_views_mariadb.sql")
            lines = ["-- MariaDB views — manual conversion to PostgreSQL required\n"]
            for v in schema.views:
                lines.append(f"-- VIEW: {v.name}\n")
                for line in v.definition.splitlines():
                    lines.append(f"-- {line}\n")
                lines.append("\n")
            p.write_text("".join(lines), encoding="utf-8")
            console.print(f"  Views extracted → [dim]{p}[/]")

        if schema.triggers:
            p = Path(f"{dst_db}_triggers_mariadb.sql")
            lines = ["-- MariaDB triggers — manual PL/pgSQL conversion required\n"]
            for t in schema.triggers:
                lines.append(
                    f"-- TRIGGER: {t.name} {t.timing} {t.event} ON {t.table}\n"
                )
                for line in t.body.splitlines():
                    lines.append(f"-- {line}\n")
                lines.append("\n")
            p.write_text("".join(lines), encoding="utf-8")
            console.print(f"  Triggers extracted → [dim]{p}[/]")

        if schema.routines:
            p = Path(f"{dst_db}_routines_mariadb.sql")
            lines = ["-- MariaDB routines — manual PL/pgSQL conversion required\n"]
            for r in schema.routines:
                lines.append(f"-- {r.routine_type}: {r.name}\n")
                for line in r.body.splitlines():
                    lines.append(f"-- {line}\n")
                lines.append("\n")
            p.write_text("".join(lines), encoding="utf-8")
            console.print(f"  Routines extracted → [dim]{p}[/]")
