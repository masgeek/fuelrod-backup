"""DDL rewriting: MariaDB CREATE TABLE SQL → PostgreSQL DDL."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class TransformResult:
    ddl: str
    post_ddl: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class SqlTransformer:
    """Rewrites a raw MariaDB CREATE TABLE statement into PostgreSQL DDL.

    Each rule is a discrete method.  transform_create_table() applies them in order.
    The result contains:
      ddl       — the cleaned CREATE TABLE statement
      post_ddl  — ALTER SEQUENCE, CREATE INDEX, COMMENT ON … (applied after data load)
      warnings  — human-readable notes for the migration report
    """

    def transform_create_table(
        self,
        raw_ddl: str,
        table_name: str,
        target_schema: str = "public",
        *,
        unsigned_checks: bool = False,
        enum_as_type: bool = False,
    ) -> TransformResult:
        post_ddl: list[str] = []
        warnings: list[str] = []

        ddl = raw_ddl

        # Strip trailing semicolon for easier manipulation; we'll add it back.
        ddl = ddl.rstrip().rstrip(";").strip()

        ddl = self._rewrite_backticks(ddl)
        ddl = self._strip_engine(ddl)
        ddl = self._strip_charset(ddl)
        ddl, seq_post = self._extract_autoincrement_start(ddl, table_name, target_schema)
        post_ddl.extend(seq_post)
        ddl = self._rewrite_autoincrement_col(ddl)
        ddl, enum_post, enum_warnings = self._rewrite_enum(
            ddl, table_name, target_schema, as_type=enum_as_type
        )
        post_ddl.extend(enum_post)
        warnings.extend(enum_warnings)
        ddl, set_post = self._rewrite_set(ddl, table_name)
        post_ddl.extend(set_post)
        ddl = self._rewrite_bit_default(ddl)
        ddl, on_update_warnings = self._strip_on_update(ddl, table_name)
        warnings.extend(on_update_warnings)
        ddl, idx_post, idx_warnings = self._extract_indexes(ddl, table_name, target_schema)
        post_ddl.extend(idx_post)
        warnings.extend(idx_warnings)
        ddl = self._rewrite_unique(ddl)
        ddl = self._rewrite_fk(ddl)
        ddl, col_comments = self._extract_col_comments(ddl, table_name, target_schema)
        post_ddl.extend(col_comments)
        ddl, tbl_comment = self._extract_table_comment(ddl, table_name, target_schema)
        if tbl_comment:
            post_ddl.append(tbl_comment)
        ddl = self._strip_col_charset(ddl)

        if unsigned_checks:
            unsigned_stmts = self._generate_unsigned_checks(ddl, table_name, target_schema)
            post_ddl.extend(unsigned_stmts)

        zerofill_cols = self._detect_zerofill(raw_ddl)
        for col in zerofill_cols:
            warnings.append(
                f"Column '{col}' had ZEROFILL — removed. "
                f"PostgreSQL has no equivalent; use application-level formatting."
            )
        ddl = self._strip_zerofill(ddl)

        # Normalise CREATE TABLE header to use double-quoted schema.table
        ddl = self._rewrite_table_header(ddl, table_name, target_schema)

        return TransformResult(ddl=ddl + ";", post_ddl=post_ddl, warnings=warnings)

    # ──────────────────────────────────────────────────────────────────────────
    #  Individual rewrite rules
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _rewrite_backticks(ddl: str) -> str:
        return re.sub(r"`([^`]+)`", lambda m: f'"{m.group(1)}"', ddl)

    @staticmethod
    def _strip_engine(ddl: str) -> str:
        return re.sub(r"\bENGINE\s*=\s*\w+", "", ddl, flags=re.IGNORECASE)

    @staticmethod
    def _strip_charset(ddl: str) -> str:
        ddl = re.sub(
            r"\b(DEFAULT\s+)?(CHARACTER\s+SET|CHARSET)\s*=?\s*\w+",
            "", ddl, flags=re.IGNORECASE,
        )
        ddl = re.sub(r"\bCOLLATE\s*=?\s*\S+", "", ddl, flags=re.IGNORECASE)
        return ddl

    @staticmethod
    def _rewrite_autoincrement_col(ddl: str) -> str:
        return re.sub(
            r"\bAUTO_INCREMENT\b",
            "GENERATED ALWAYS AS IDENTITY",
            ddl, flags=re.IGNORECASE,
        )

    @staticmethod
    def _extract_autoincrement_start(
        ddl: str, table_name: str, target_schema: str
    ) -> tuple[str, list[str]]:
        m = re.search(r"\bAUTO_INCREMENT\s*=\s*(\d+)", ddl, re.IGNORECASE)
        post: list[str] = []
        if m:
            start = int(m.group(1))
            if start > 1:
                post.append(
                    f'-- Sequence reset for {target_schema}.{table_name} '
                    f'(applied after data load via setval)'
                )
                post.append(
                    f'SELECT setval(pg_get_serial_sequence('
                    f'\'"{ target_schema }"."{table_name}"\', \'id\'), '
                    f'GREATEST({start - 1}, COALESCE(MAX(id), {start - 1}))) '
                    f'FROM "{target_schema}"."{table_name}";'
                )
            ddl = re.sub(r"\bAUTO_INCREMENT\s*=\s*\d+", "", ddl, flags=re.IGNORECASE)
        return ddl, post

    @staticmethod
    def _rewrite_enum(
        ddl: str,
        table_name: str,
        target_schema: str,
        *,
        as_type: bool = False,
    ) -> tuple[str, list[str], list[str]]:
        post: list[str] = []
        warnings: list[str] = []

        def replace_enum(m: re.Match) -> str:
            col_name = m.group(1).strip().strip('"')
            raw_values = m.group(2)
            values = re.findall(r"'([^']*)'", raw_values)
            if as_type:
                type_name = f"{table_name}_{col_name}_enum"
                post.append(
                    f"DO $$ BEGIN CREATE TYPE \"{target_schema}\".\"{type_name}\" "
                    f"AS ENUM ({', '.join(repr(v) for v in values)}); "
                    f"EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
                )
                return f'"{col_name}" "{target_schema}"."{type_name}"'
            else:
                check_values = ", ".join(f"'{v}'" for v in values)
                post.append(
                    f'ALTER TABLE "{target_schema}"."{table_name}" '
                    f'ADD CONSTRAINT "{table_name}_{col_name}_chk" '
                    f'CHECK ("{col_name}" IN ({check_values}));'
                )
                return f'"{col_name}" TEXT'

        pattern = re.compile(
            r'"(\w+)"\s+(?:enum|ENUM)\(([^)]+)\)',
            re.IGNORECASE,
        )
        ddl = pattern.sub(replace_enum, ddl)
        return ddl, post, warnings

    @staticmethod
    def _rewrite_set(ddl: str, table_name: str) -> tuple[str, list[str]]:
        post: list[str] = []

        def replace_set(m: re.Match) -> str:
            col_name = m.group(1).strip().strip('"')
            raw_values = m.group(2)
            values = re.findall(r"'([^']*)'", raw_values)
            check_values = ", ".join(f"'{v}'" for v in values)
            post.append(
                f'-- SET column "{col_name}": values allowed: {check_values}'
            )
            return f'"{col_name}" TEXT'

        pattern = re.compile(r'"(\w+)"\s+(?:set|SET)\(([^)]+)\)', re.IGNORECASE)
        ddl = pattern.sub(replace_set, ddl)
        return ddl, post

    @staticmethod
    def _rewrite_bit_default(ddl: str) -> str:
        return re.sub(r"DEFAULT\s+b'(\d+)'", lambda m: f"DEFAULT '{m.group(1)}'::bit", ddl)

    @staticmethod
    def _strip_on_update(ddl: str, table_name: str) -> tuple[str, list[str]]:
        warnings: list[str] = []
        matches = re.findall(r'"(\w+)"[^,\n]*ON\s+UPDATE\s+CURRENT_TIMESTAMP', ddl, re.IGNORECASE)
        for col in matches:
            warnings.append(
                f"Column '{col}' had ON UPDATE CURRENT_TIMESTAMP — removed. "
                f"Add a BEFORE UPDATE trigger in PL/pgSQL to replicate this behaviour."
            )
        ddl = re.sub(r"\bON\s+UPDATE\s+CURRENT_TIMESTAMP\b", "", ddl, flags=re.IGNORECASE)
        return ddl, warnings

    @staticmethod
    def _extract_indexes(
        ddl: str, table_name: str, target_schema: str
    ) -> tuple[str, list[str], list[str]]:
        post: list[str] = []
        warnings: list[str] = []
        lines_to_remove: list[str] = []

        for line in ddl.splitlines():
            stripped = line.strip().rstrip(",")

            fulltext_m = re.match(
                r'(?:FULLTEXT\s+)?(?:FULLTEXT)\s+(?:KEY|INDEX)\s*"?(\w+)"?\s*\(([^)]+)\)',
                stripped, re.IGNORECASE,
            )
            if fulltext_m:
                idx_name = fulltext_m.group(1)
                cols = fulltext_m.group(2)
                col_list = ", ".join(
                    f'to_tsvector(\'english\', {c.strip()})' for c in cols.split(",")
                )
                warnings.append(
                    f"FULLTEXT index '{idx_name}' on {table_name} requires manual migration. "
                    f"Suggested: CREATE INDEX \"{idx_name}_fts\" ON \"{target_schema}\".\"{table_name}\" "
                    f"USING gin({col_list});"
                )
                post.append(
                    f'-- FULLTEXT index "{idx_name}" on {table_name} — review and enable manually:\n'
                    f'-- CREATE INDEX "{idx_name}_fts" ON "{target_schema}"."{table_name}" '
                    f'USING gin({col_list});'
                )
                lines_to_remove.append(line)
                continue

            key_m = re.match(
                r'(?:KEY|INDEX)\s+"?(\w+)"?\s*\(([^)]+)\)',
                stripped, re.IGNORECASE,
            )
            if key_m:
                idx_name = key_m.group(1)
                cols_raw = key_m.group(2)
                col_list = ", ".join(
                    f'"{c.strip().strip(chr(34))}"' for c in cols_raw.split(",")
                )
                post.append(
                    f'CREATE INDEX IF NOT EXISTS "{idx_name}" '
                    f'ON "{target_schema}"."{table_name}" ({col_list});'
                )
                lines_to_remove.append(line)

        for line in lines_to_remove:
            ddl = ddl.replace(line + "\n", "").replace(line, "")

        return ddl, post, warnings

    @staticmethod
    def _rewrite_unique(ddl: str) -> str:
        return re.sub(
            r'\bUNIQUE\s+KEY\s+"?\w+"?\s*(\([^)]+\))',
            lambda m: f"UNIQUE {m.group(1)}",
            ddl, flags=re.IGNORECASE,
        )

    @staticmethod
    def _rewrite_fk(ddl: str) -> str:
        def add_deferrable(m: re.Match) -> str:
            fk = m.group(0)
            if "DEFERRABLE" not in fk.upper():
                fk = fk.rstrip() + " DEFERRABLE INITIALLY DEFERRED"
            return fk

        ddl = re.sub(
            r'FOREIGN\s+KEY[^,)]+REFERENCES[^,)]+(\([^)]+\))[^,)]*',
            add_deferrable, ddl, flags=re.IGNORECASE,
        )
        return ddl

    @staticmethod
    def _extract_col_comments(
        ddl: str, table_name: str, target_schema: str
    ) -> tuple[str, list[str]]:
        post: list[str] = []

        def replacer(m: re.Match) -> str:
            col_name = m.group(1)
            comment = m.group(2)
            post.append(
                f'COMMENT ON COLUMN "{target_schema}"."{table_name}"."{col_name}" '
                f"IS '{comment.replace(chr(39), chr(39)*2)}';",
            )
            return ""

        ddl = re.sub(r'"(\w+)"[^,\n]*\s+COMMENT\s+\'([^\']+)\'', replacer, ddl, flags=re.IGNORECASE)
        return ddl, post

    @staticmethod
    def _extract_table_comment(
        ddl: str, table_name: str, target_schema: str
    ) -> tuple[str, str | None]:
        m = re.search(r"COMMENT\s*=\s*'([^']*)'", ddl, re.IGNORECASE)
        if m:
            comment = m.group(1).replace("'", "''")
            stmt = (
                f'COMMENT ON TABLE "{target_schema}"."{table_name}" IS \'{comment}\';'
            )
            ddl = re.sub(r"COMMENT\s*=\s*'[^']*'", "", ddl, flags=re.IGNORECASE)
            return ddl, stmt
        return ddl, None

    @staticmethod
    def _strip_col_charset(ddl: str) -> str:
        ddl = re.sub(r"\bCHARACTER\s+SET\s+\S+", "", ddl, flags=re.IGNORECASE)
        ddl = re.sub(r"\bCHARSET\s+\S+", "", ddl, flags=re.IGNORECASE)
        ddl = re.sub(r"\bCOLLATE\s+\S+", "", ddl, flags=re.IGNORECASE)
        return ddl

    @staticmethod
    def _detect_zerofill(ddl: str) -> list[str]:
        cols: list[str] = []
        for m in re.finditer(r'"(\w+)"[^,\n]*\bZEROFILL\b', ddl, re.IGNORECASE):
            cols.append(m.group(1))
        return cols

    @staticmethod
    def _strip_zerofill(ddl: str) -> str:
        return re.sub(r"\bZEROFILL\b", "", ddl, flags=re.IGNORECASE)

    @staticmethod
    def _generate_unsigned_checks(
        ddl: str, table_name: str, target_schema: str
    ) -> list[str]:
        stmts: list[str] = []
        for m in re.finditer(r'"(\w+)"[^,\n]*\bUNSIGNED\b', ddl, re.IGNORECASE):
            col = m.group(1)
            stmts.append(
                f'ALTER TABLE "{target_schema}"."{table_name}" '
                f'ADD CONSTRAINT "{table_name}_{col}_unsigned_chk" '
                f'CHECK ("{col}" >= 0);'
            )
        return stmts

    @staticmethod
    def _rewrite_table_header(ddl: str, table_name: str, target_schema: str) -> str:
        return re.sub(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`[^`]+`|\"[^\"]+\"|\w+)",
            f'CREATE TABLE IF NOT EXISTS "{target_schema}"."{table_name}"',
            ddl, count=1, flags=re.IGNORECASE,
        )
