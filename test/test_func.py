"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

import pytest

from simplesqlite import (
    NameValidationError,
    NullDatabaseConnectionError,
    append_table,
    connect_memdb,
    copy_table,
)
from simplesqlite._func import validate_attribute_name, validate_table_name

from .fixture import (  # noqa: W0611
    TEST_TABLE_NAME,
    con_empty,
    con_mix,
    con_null,
    con_profile,
    con_ro,
)


class Test_validate_table_name:
    @pytest.mark.parametrize(["value"], [["valid_table_name"], ["table_"], ["%CPU"]])
    def test_normal(self, value):
        validate_table_name(value)

    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            [None, NameValidationError],
            ["", NameValidationError],
            ["table", NameValidationError],
            ["TABLE", NameValidationError],
            ["Table", NameValidationError],
        ],
    )
    def test_exception(self, value, expected):
        with pytest.raises(expected):
            validate_table_name(value)


class Test_validate_attr_name:
    @pytest.mark.parametrize(["value"], [["valid_attr_name"], ["attr_"], ["%CPU"]])
    def test_normal(self, value):
        validate_attribute_name(value)

    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            [None, NameValidationError],
            ["", NameValidationError],
            ["table", NameValidationError],
            ["TABLE", NameValidationError],
            ["Table", NameValidationError],
        ],
    )
    def test_exception(self, value, expected):
        with pytest.raises(expected):
            validate_attribute_name(value)


class Test_append_table:
    def test_normal(self, con_mix, con_empty):
        assert append_table(src_db_con=con_mix, dst_db_con=con_empty, table_name=TEST_TABLE_NAME)

        result = con_mix.select(select="*", table_name=TEST_TABLE_NAME)
        src_data_matrix = result.fetchall()
        result = con_empty.select(select="*", table_name=TEST_TABLE_NAME)
        dst_data_matrix = result.fetchall()

        assert src_data_matrix == dst_data_matrix
        assert append_table(src_db_con=con_mix, dst_db_con=con_empty, table_name=TEST_TABLE_NAME)

        result = con_mix.select(select="*", table_name=TEST_TABLE_NAME)
        src_data_matrix = result.fetchall()
        result = con_empty.select(select="*", table_name=TEST_TABLE_NAME)
        dst_data_matrix = result.fetchall()

        assert src_data_matrix * 2 == dst_data_matrix

    def test_exception_mismatch_schema(self, con_mix, con_profile):
        with pytest.raises(ValueError):
            append_table(src_db_con=con_mix, dst_db_con=con_profile, table_name=TEST_TABLE_NAME)

    def test_exception_null_connection(self, con_mix, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            append_table(src_db_con=con_mix, dst_db_con=con_null, table_name=TEST_TABLE_NAME)

    def test_exception_permission(self, con_mix, con_ro):
        with pytest.raises(IOError):
            append_table(src_db_con=con_mix, dst_db_con=con_ro, table_name=TEST_TABLE_NAME)


class Test_copy_table:
    def test_normal(self, con_mix, con_empty):
        assert copy_table(
            src_db_con=con_mix, dst_db_con=con_empty, src_table_name=TEST_TABLE_NAME, dst_table_name="dst"
        )

        result = con_mix.select(select="*", table_name=TEST_TABLE_NAME)
        src_data_matrix = result.fetchall()
        result = con_empty.select(select="*", table_name="dst")
        dst_data_matrix = result.fetchall()

        assert src_data_matrix == dst_data_matrix

        assert not copy_table(
            src_db_con=con_mix,
            dst_db_con=con_empty,
            src_table_name=TEST_TABLE_NAME,
            dst_table_name="dst",
            is_overwrite=False,
        )
        assert copy_table(
            src_db_con=con_mix,
            dst_db_con=con_empty,
            src_table_name=TEST_TABLE_NAME,
            dst_table_name="dst",
            is_overwrite=True,
        )


class Test_connect_sqlite_db_mem:
    def test_normal(self):
        con_mem = connect_memdb()
        assert con_mem is not None
        assert con_mem.database_path == ":memory:"
