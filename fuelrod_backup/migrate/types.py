"""MariaDB → PostgreSQL type mapping."""

from __future__ import annotations

import re


class TypeMapper:
    """Stateless mapper from MariaDB column metadata to a PostgreSQL type string."""

    def map(
        self,
        data_type: str,
        column_type: str,
        is_unsigned: bool,
        extra: str,
        character_maximum_length: int | None = None,
        numeric_precision: int | None = None,
        numeric_scale: int | None = None,
    ) -> tuple[str, list[str]]:
        """Return (pg_type, warnings).

        data_type   — e.g. "int"  (from information_schema.COLUMNS.DATA_TYPE)
        column_type — e.g. "int(10) unsigned"  (from COLUMN_TYPE, richer)
        is_unsigned — derived from 'unsigned' in column_type
        extra       — e.g. "auto_increment"
        """
        warnings: list[str] = []
        dt = data_type.lower().strip()
        ct = column_type.lower().strip()

        # ── Integer types ────────────────────────────────────────────────────
        if dt == "tinyint":
            if character_maximum_length == 1 or "tinyint(1)" in ct:
                return "BOOLEAN", warnings
            return ("SMALLINT" if is_unsigned else "SMALLINT"), warnings

        if dt == "smallint":
            return ("INTEGER" if is_unsigned else "SMALLINT"), warnings

        if dt in ("mediumint",):
            return "INTEGER", warnings

        if dt in ("int", "integer"):
            return ("BIGINT" if is_unsigned else "INTEGER"), warnings

        if dt == "bigint":
            if is_unsigned:
                warnings.append(
                    f"BIGINT UNSIGNED column mapped to NUMERIC(20,0) — "
                    f"values above 9223372036854775807 cannot fit in PG BIGINT"
                )
                return "NUMERIC(20,0)", warnings
            return "BIGINT", warnings

        # ── Floating point ───────────────────────────────────────────────────
        if dt == "float":
            return "REAL", warnings

        if dt in ("double", "double precision", "real"):
            return "DOUBLE PRECISION", warnings

        # ── Fixed-precision ──────────────────────────────────────────────────
        if dt in ("decimal", "numeric"):
            if numeric_precision is not None and numeric_scale is not None:
                return f"NUMERIC({numeric_precision},{numeric_scale})", warnings
            if numeric_precision is not None:
                return f"NUMERIC({numeric_precision})", warnings
            return "NUMERIC", warnings

        # ── Bit ──────────────────────────────────────────────────────────────
        if dt == "bit":
            m = re.search(r'bit\s*\((\d+)\)', ct)
            width = int(m.group(1)) if m else 1
            if width > 1:
                return f"BIT({width})", warnings
            return "BOOLEAN", warnings

        # ── Character types ──────────────────────────────────────────────────
        if dt == "char":
            length = character_maximum_length or 1
            return f"CHAR({length})", warnings

        if dt == "varchar":
            length = character_maximum_length or 255
            return f"VARCHAR({length})", warnings

        if dt in ("tinytext", "text", "mediumtext", "longtext"):
            return "TEXT", warnings

        # ── Binary / blob ────────────────────────────────────────────────────
        if dt in ("tinyblob", "blob", "mediumblob", "longblob", "binary", "varbinary"):
            return "BYTEA", warnings

        # ── Date / time ──────────────────────────────────────────────────────
        if dt == "date":
            return "DATE", warnings

        if dt == "time":
            return "TIME WITHOUT TIME ZONE", warnings

        if dt == "datetime":
            return "TIMESTAMP WITHOUT TIME ZONE", warnings

        if dt == "timestamp":
            return "TIMESTAMP WITH TIME ZONE", warnings

        if dt == "year":
            return "SMALLINT", warnings

        # ── JSON ─────────────────────────────────────────────────────────────
        if dt == "json":
            return "JSONB", warnings

        # ── ENUM — handled upstream by SqlTransformer; TypeMapper returns TEXT ─
        if dt == "enum":
            return "TEXT", warnings

        # ── SET — handled upstream; TypeMapper returns TEXT ──────────────────
        if dt == "set":
            return "TEXT", warnings

        # ── Spatial ──────────────────────────────────────────────────────────
        if dt in (
            "geometry", "point", "linestring", "polygon",
            "multipoint", "multilinestring", "multipolygon", "geometrycollection",
        ):
            warnings.append(
                f"Spatial type '{data_type}' mapped to TEXT. "
                f"Install PostGIS and convert manually for proper spatial support."
            )
            return "TEXT", warnings

        # ── Fallback ─────────────────────────────────────────────────────────
        warnings.append(f"Unknown MariaDB type '{data_type}' — mapped to TEXT. Verify manually.")
        return "TEXT", warnings
