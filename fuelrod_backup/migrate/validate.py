"""Post-migration validation: row counts and optional MD5 checksums."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ValidationResult:
    table: str
    rows_source: int = 0
    rows_dest: int = 0
    count_match: bool = False
    checksum_source: str | None = None
    checksum_dest: str | None = None
    checksum_match: bool | None = None  # None = not run
    errors: list[str] = field(default_factory=list)


# Types where checksum comparison is unreliable or unsafe
_SKIP_CHECKSUM_TYPES = frozenset({
    "float", "double", "real", "double precision",
    "bytea", "blob", "tinyblob", "mediumblob", "longblob",
    "binary", "varbinary",
    "geometry", "point", "linestring", "polygon",
})


class Validator:
    """Validates migrated table data by comparing source and destination."""

    def validate_table(
        self,
        table: str,
        src_adapter,
        pg_runner,
        src_db: str,
        dst_db: str,
        target_schema: str = "public",
        pk_cols: list[str] | None = None,
        column_names: list[str] | None = None,
        column_types: list[str] | None = None,
        *,
        checksums: bool = False,
    ) -> ValidationResult:
        result = ValidationResult(table=table)
        try:
            result.rows_source = int(
                src_adapter._query_one(f"SELECT COUNT(*) FROM `{table}`", dbname=src_db)  # noqa: S608
            )
        except Exception as exc:
            result.errors.append(f"source count error: {exc}")
            return result

        try:
            result.rows_dest = int(
                pg_runner._query_one(
                    f'SELECT COUNT(*) FROM "{target_schema}"."{table}"',  # noqa: S608
                    dbname=dst_db,
                )
            )
        except Exception as exc:
            result.errors.append(f"dest count error: {exc}")
            return result

        result.count_match = result.rows_source == result.rows_dest

        if checksums and pk_cols and column_names:
            result.checksum_source, result.checksum_dest, result.checksum_match = (
                self._compare_checksums(
                    table, src_adapter, pg_runner, src_db, dst_db,
                    target_schema, pk_cols, column_names, column_types or [],
                )
            )

        return result

    def _compare_checksums(
        self,
        table: str,
        src_adapter,
        pg_runner,
        src_db: str,
        dst_db: str,
        target_schema: str,
        pk_cols: list[str],
        column_names: list[str],
        column_types: list[str],
    ) -> tuple[str | None, str | None, bool | None]:
        # Skip checksum for tables with unreliable types
        for ct in column_types:
            if ct.lower() in _SKIP_CHECKSUM_TYPES:
                return None, None, None

        pk_order_src = ", ".join(f"`{c}`" for c in pk_cols)
        pk_order_dst = ", ".join(f'"{c}"' for c in pk_cols)
        cols_src = ", ".join(f"`{c}`" for c in column_names)
        cols_dst = ", ".join(f'"{c}"::text' for c in column_names)
        concat_sep = "|"

        sql_src = (
            f"SELECT MD5(GROUP_CONCAT("  # noqa: S608
            f"MD5(CONCAT_WS('{concat_sep}', {cols_src})) "
            f"ORDER BY {pk_order_src}"
            f")) AS chk FROM `{table}`"
        )
        sql_dst = (
            f"SELECT MD5(STRING_AGG("  # noqa: S608
            f"MD5(CONCAT_WS('{concat_sep}', {cols_dst})), "
            f"'' ORDER BY {pk_order_dst}"
            f")) AS chk FROM \"{target_schema}\".\"{table}\""
        )

        try:
            src_cksum = src_adapter._query_one(sql_src, dbname=src_db) or None
        except Exception:
            return None, None, None

        try:
            dst_cksum = pg_runner._query_one(sql_dst, dbname=dst_db) or None
        except Exception:
            return src_cksum, None, None

        match = (src_cksum == dst_cksum) if (src_cksum and dst_cksum) else None
        return src_cksum, dst_cksum, match
