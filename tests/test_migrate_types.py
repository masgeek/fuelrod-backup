"""Unit tests for TypeMapper — MariaDB → PostgreSQL type conversion."""

import pytest

from fuelrod_backup.migrate.types import TypeMapper


@pytest.fixture
def mapper():
    return TypeMapper()


def _m(mapper, data_type, column_type=None, unsigned=False, extra="", char_len=None, precision=None, scale=None):
    return mapper.map(
        data_type=data_type,
        column_type=column_type or data_type,
        is_unsigned=unsigned,
        extra=extra,
        character_maximum_length=char_len,
        numeric_precision=precision,
        numeric_scale=scale,
    )


class TestIntegerTypes:
    def test_tinyint_1_maps_to_boolean(self, mapper):
        pg_type, warnings = _m(mapper, "tinyint", "tinyint(1)")
        assert pg_type == "BOOLEAN"
        assert not warnings

    def test_tinyint_maps_to_smallint(self, mapper):
        pg_type, _ = _m(mapper, "tinyint", "tinyint(4)")
        assert pg_type == "SMALLINT"

    def test_tinyint_unsigned_maps_to_smallint(self, mapper):
        pg_type, _ = _m(mapper, "tinyint", "tinyint(3) unsigned", unsigned=True)
        assert pg_type == "SMALLINT"

    def test_smallint_maps_to_smallint(self, mapper):
        pg_type, _ = _m(mapper, "smallint")
        assert pg_type == "SMALLINT"

    def test_smallint_unsigned_maps_to_integer(self, mapper):
        pg_type, _ = _m(mapper, "smallint", unsigned=True)
        assert pg_type == "INTEGER"

    def test_mediumint_maps_to_integer(self, mapper):
        pg_type, _ = _m(mapper, "mediumint")
        assert pg_type == "INTEGER"

    def test_int_maps_to_integer(self, mapper):
        pg_type, _ = _m(mapper, "int")
        assert pg_type == "INTEGER"

    def test_int_unsigned_maps_to_bigint(self, mapper):
        pg_type, _ = _m(mapper, "int", unsigned=True)
        assert pg_type == "BIGINT"

    def test_bigint_maps_to_bigint(self, mapper):
        pg_type, _ = _m(mapper, "bigint")
        assert pg_type == "BIGINT"

    def test_bigint_unsigned_maps_to_numeric_with_warning(self, mapper):
        pg_type, warnings = _m(mapper, "bigint", "bigint(20) unsigned", unsigned=True)
        assert pg_type == "NUMERIC(20,0)"
        assert warnings
        assert "NUMERIC" in warnings[0]


class TestFloatTypes:
    def test_float_maps_to_real(self, mapper):
        pg_type, _ = _m(mapper, "float")
        assert pg_type == "REAL"

    def test_double_maps_to_double_precision(self, mapper):
        pg_type, _ = _m(mapper, "double")
        assert pg_type == "DOUBLE PRECISION"

    def test_decimal_preserves_precision(self, mapper):
        pg_type, _ = _m(mapper, "decimal", precision=10, scale=2)
        assert pg_type == "NUMERIC(10,2)"

    def test_decimal_no_precision(self, mapper):
        pg_type, _ = _m(mapper, "decimal")
        assert pg_type == "NUMERIC"


class TestStringTypes:
    def test_char_preserves_length(self, mapper):
        pg_type, _ = _m(mapper, "char", char_len=10)
        assert pg_type == "CHAR(10)"

    def test_varchar_preserves_length(self, mapper):
        pg_type, _ = _m(mapper, "varchar", char_len=255)
        assert pg_type == "VARCHAR(255)"

    def test_text_maps_to_text(self, mapper):
        for dt in ("tinytext", "text", "mediumtext", "longtext"):
            pg_type, _ = _m(mapper, dt)
            assert pg_type == "TEXT", f"{dt} should map to TEXT"


class TestBinaryTypes:
    def test_blob_maps_to_bytea(self, mapper):
        for dt in ("tinyblob", "blob", "mediumblob", "longblob", "binary", "varbinary"):
            pg_type, _ = _m(mapper, dt)
            assert pg_type == "BYTEA", f"{dt} should map to BYTEA"


class TestDateTimeTypes:
    def test_date_maps_to_date(self, mapper):
        pg_type, _ = _m(mapper, "date")
        assert pg_type == "DATE"

    def test_time_maps_to_time_without_tz(self, mapper):
        pg_type, _ = _m(mapper, "time")
        assert pg_type == "TIME WITHOUT TIME ZONE"

    def test_datetime_maps_to_timestamp_without_tz(self, mapper):
        pg_type, _ = _m(mapper, "datetime")
        assert pg_type == "TIMESTAMP WITHOUT TIME ZONE"

    def test_timestamp_maps_to_timestamp_with_tz(self, mapper):
        pg_type, _ = _m(mapper, "timestamp")
        assert pg_type == "TIMESTAMP WITH TIME ZONE"

    def test_year_maps_to_smallint(self, mapper):
        pg_type, _ = _m(mapper, "year")
        assert pg_type == "SMALLINT"


class TestSpecialTypes:
    def test_json_maps_to_jsonb(self, mapper):
        pg_type, _ = _m(mapper, "json")
        assert pg_type == "JSONB"

    def test_enum_maps_to_text(self, mapper):
        pg_type, _ = _m(mapper, "enum", "enum('a','b')")
        assert pg_type == "TEXT"

    def test_set_maps_to_text(self, mapper):
        pg_type, _ = _m(mapper, "set", "set('x','y')")
        assert pg_type == "TEXT"

    def test_bit_preserves_width(self, mapper):
        pg_type, _ = _m(mapper, "bit", "bit(8)")
        assert pg_type == "BIT(8)"

    def test_geometry_maps_to_text_with_warning(self, mapper):
        pg_type, warnings = _m(mapper, "geometry")
        assert pg_type == "TEXT"
        assert warnings
        assert "PostGIS" in warnings[0]

    def test_unknown_type_maps_to_text_with_warning(self, mapper):
        pg_type, warnings = _m(mapper, "unknown_type_xyz")
        assert pg_type == "TEXT"
        assert warnings
