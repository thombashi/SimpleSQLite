# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

import pytest

from simplesqlite import InvalidTableNameError
from simplesqlite.loader.data import TableData


class Test_TableData:

    @pytest.mark.parametrize(
        ["table_name", "header_list", "record_list", "expected"], [
            [
                "tablename", ["a", "b"], [],
                TableData("tablename", ["a", "b"], [])
            ],
            [
                "tablename", ["where", "b"], [],
                TableData("tablename", ["where_rename0", "b"], [])
            ],
            [
                "tablename", ["where", "where"], [],
                TableData("tablename", ["where_rename0", "where_rename1"], [])
            ],
            [
                "tablename", ["where", "where_rename0"], [],
                TableData("tablename", ["where_rename1", "where_rename0"], [])
            ],
        ]
    )
    def test_normal(self, table_name, header_list, record_list, expected):
        tabledata = TableData(table_name, header_list, record_list)
        assert tabledata == expected

    @pytest.mark.parametrize(
        ["table_name", "header_list", "record_list", "expected"], [
            ["", ["a", "b"], [], InvalidTableNameError],
            [None, ["a", "b"], [], InvalidTableNameError],
            ["where", ["a", "b"], [], InvalidTableNameError],
        ]
    )
    def test_exception(self, table_name, header_list, record_list, expected):
        with pytest.raises(expected):
            TableData(table_name, header_list, record_list)
