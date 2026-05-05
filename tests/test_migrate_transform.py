"""Unit tests for SqlTransformer — DDL rewriting rules."""

import pytest

from fuelrod_backup.migrate.transform import SqlTransformer


@pytest.fixture
def tf():
    return SqlTransformer()


RAW_SIMPLE = """\
CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `email` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


class TestBacktickRewrite:
    def test_backticks_rewritten_to_double_quotes(self, tf):
        result = tf.transform_create_table(RAW_SIMPLE, "users")
        assert "`" not in result.ddl
        assert '"users"' in result.ddl
        assert '"id"' in result.ddl

    def test_no_double_quotes_in_original_preserved(self, tf):
        result = tf.transform_create_table(RAW_SIMPLE, "users")
        assert '"name"' in result.ddl


class TestEngineAndCharset:
    def test_engine_clause_stripped(self, tf):
        result = tf.transform_create_table(RAW_SIMPLE, "users")
        assert "ENGINE" not in result.ddl.upper()

    def test_charset_clause_stripped(self, tf):
        result = tf.transform_create_table(RAW_SIMPLE, "users")
        assert "CHARSET" not in result.ddl.upper()
        assert "utf8mb4" not in result.ddl


class TestAutoIncrement:
    def test_autoincrement_col_rewritten_to_identity(self, tf):
        result = tf.transform_create_table(RAW_SIMPLE, "users")
        assert "GENERATED ALWAYS AS IDENTITY" in result.ddl
        assert "AUTO_INCREMENT" not in result.ddl

    def test_table_autoincrement_start_in_post_ddl(self, tf):
        ddl = """\
CREATE TABLE `orders` (
  `id` int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1001;
"""
        result = tf.transform_create_table(ddl, "orders")
        assert "AUTO_INCREMENT=1001" not in result.ddl
        seq_stmts = [s for s in result.post_ddl if "setval" in s.lower() or "RESTART" in s.upper()]
        assert seq_stmts, "Sequence reset statement not found in post_ddl"


class TestEnum:
    def test_enum_becomes_text(self, tf):
        ddl = """\
CREATE TABLE `t` (
  `status` enum('active','inactive','pending') NOT NULL
) ENGINE=InnoDB;
"""
        result = tf.transform_create_table(ddl, "t")
        assert "TEXT" in result.ddl
        assert "enum" not in result.ddl.lower()

    def test_enum_check_constraint_in_post_ddl(self, tf):
        ddl = """\
CREATE TABLE `t` (
  `status` enum('active','inactive') NOT NULL
) ENGINE=InnoDB;
"""
        result = tf.transform_create_table(ddl, "t")
        checks = [s for s in result.post_ddl if "CHECK" in s.upper() and "status" in s]
        assert checks, "No CHECK constraint for enum in post_ddl"
        assert "'active'" in checks[0]
        assert "'inactive'" in checks[0]


class TestOnUpdateCurrentTimestamp:
    def test_on_update_stripped_with_warning(self, tf):
        ddl = """\
CREATE TABLE `t` (
  `updated_at` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;
"""
        result = tf.transform_create_table(ddl, "t")
        assert "ON UPDATE" not in result.ddl.upper()
        assert result.warnings
        assert any("ON UPDATE" in w for w in result.warnings)


class TestIndexExtraction:
    def test_regular_key_extracted_to_post_ddl(self, tf):
        result = tf.transform_create_table(RAW_SIMPLE, "users")
        idx_stmts = [s for s in result.post_ddl if "CREATE INDEX" in s.upper()]
        assert idx_stmts, "Index CREATE statement not found in post_ddl"
        assert '"idx_email"' in idx_stmts[0]

    def test_fulltext_key_becomes_warning_and_comment(self, tf):
        ddl = """\
CREATE TABLE `posts` (
  `id` int NOT NULL,
  `body` text,
  FULLTEXT KEY `ft_body` (`body`)
) ENGINE=InnoDB;
"""
        result = tf.transform_create_table(ddl, "posts")
        assert result.warnings
        assert any("FULLTEXT" in w for w in result.warnings)
        fulltext_comments = [s for s in result.post_ddl if "FULLTEXT" in s]
        assert fulltext_comments


class TestZerofill:
    def test_zerofill_stripped_with_warning(self, tf):
        ddl = """\
CREATE TABLE `t` (
  `code` int(5) UNSIGNED ZEROFILL NOT NULL
) ENGINE=InnoDB;
"""
        result = tf.transform_create_table(ddl, "t")
        assert "ZEROFILL" not in result.ddl.upper()
        assert result.warnings
        assert any("ZEROFILL" in w for w in result.warnings)


class TestForeignKeys:
    def test_fk_gets_deferrable(self, tf):
        ddl = """\
CREATE TABLE `orders` (
  `id` int NOT NULL,
  `user_id` int NOT NULL,
  CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB;
"""
        result = tf.transform_create_table(ddl, "orders")
        assert "DEFERRABLE INITIALLY DEFERRED" in result.ddl

    def test_backtick_fk_references_rewritten(self, tf):
        ddl = """\
CREATE TABLE `orders` (
  `id` int NOT NULL,
  `user_id` int NOT NULL,
  CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB;
"""
        result = tf.transform_create_table(ddl, "orders")
        assert "`users`" not in result.ddl
        assert '"users"' in result.ddl


class TestTableHeader:
    def test_table_header_uses_schema_and_double_quotes(self, tf):
        result = tf.transform_create_table(RAW_SIMPLE, "users", target_schema="myschema")
        assert '"myschema"."users"' in result.ddl

    def test_if_not_exists_present(self, tf):
        result = tf.transform_create_table(RAW_SIMPLE, "users")
        assert "IF NOT EXISTS" in result.ddl.upper()

    def test_result_ends_with_semicolon(self, tf):
        result = tf.transform_create_table(RAW_SIMPLE, "users")
        assert result.ddl.rstrip().endswith(";")
