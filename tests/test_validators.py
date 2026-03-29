"""Tests for _validate_identifier in the MSSQL and MariaDB adapters."""

import pytest

from fuelrod_backup.adapters.mariadb import _validate_identifier as mariadb_validate
from fuelrod_backup.adapters.mssql import _validate_identifier as mssql_validate


# Both adapters share the same logic — parametrize over both implementations.
@pytest.fixture(params=["mssql", "mariadb"])
def validate(request):
    return mssql_validate if request.param == "mssql" else mariadb_validate


# ──────────────────────────────────────────────────────────────────────────────
#  Valid identifiers — should not raise
# ──────────────────────────────────────────────────────────────────────────────

class TestValidIdentifiers:
    def test_simple_name(self, validate):
        validate("mydatabase")  # no exception

    def test_uppercase(self, validate):
        validate("MyDatabase")

    def test_with_numbers(self, validate):
        validate("db123")

    def test_with_underscore(self, validate):
        validate("my_database")

    def test_with_hyphen(self, validate):
        validate("my-database")

    def test_with_space(self, validate):
        # Database names with spaces are valid on both engines.
        validate("my database")

    def test_mixed(self, validate):
        validate("My-DB_2024 prod")


# ──────────────────────────────────────────────────────────────────────────────
#  Invalid identifiers — must raise ValueError
# ──────────────────────────────────────────────────────────────────────────────

class TestInvalidIdentifiers:
    def test_empty_string(self, validate):
        with pytest.raises(ValueError):
            validate("")

    def test_semicolon(self, validate):
        with pytest.raises(ValueError):
            validate("db; DROP DATABASE master; --")

    def test_single_quote(self, validate):
        with pytest.raises(ValueError):
            validate("db'injection")

    def test_double_quote(self, validate):
        with pytest.raises(ValueError):
            validate('db"injection')

    def test_closing_bracket(self, validate):
        with pytest.raises(ValueError):
            validate("db]injection")

    def test_backtick(self, validate):
        with pytest.raises(ValueError):
            validate("db`injection")

    def test_newline(self, validate):
        with pytest.raises(ValueError):
            validate("db\ninjection")

    def test_null_byte(self, validate):
        with pytest.raises(ValueError):
            validate("db\x00injection")


# ──────────────────────────────────────────────────────────────────────────────
#  Error message content
# ──────────────────────────────────────────────────────────────────────────────

class TestErrorMessage:
    def test_error_includes_name(self, validate):
        with pytest.raises(ValueError, match="bad;name"):
            validate("bad;name")

    def test_error_includes_custom_label(self, validate):
        with pytest.raises(ValueError, match="database name"):
            validate("bad;name", "database name")
