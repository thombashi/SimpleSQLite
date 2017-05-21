# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import unicode_literals

from decimal import Decimal

from pytablereader import (
    TableData,
    InvalidDataError,
)
import pytest
from simplesqlite import connect_sqlite_db_mem
from simplesqlite.sqlquery import SqlQuery
import typepy


try:
    import pandas
    PANDAS_IMPORT = True
except ImportError:
    PANDAS_IMPORT = False


@pytest.mark.skipif("PANDAS_IMPORT is False")
class Test_TableData_from_dataframe(object):

    def test_normal(self):
        con = connect_sqlite_db_mem()
        dataframe = pandas.DataFrame(
            [
                [0, 0.1, "a"],
                [1, 1.1, "bb"],
                [2, 2.2, "ccc"],
            ],
            columns=['id', 'value', 'name']
        )

        con.create_table_from_dataframe(dataframe, "tablename")
        result = con.select(
            select=", ".join(
                SqlQuery.to_attr_str_list(['id', 'value', 'name'])),
            table_name="tablename")

        assert result.fetchall() == [
            (0, 0.1, u'a'),
            (1, 1.1, u'bb'),
            (2, 2.2, u'ccc'),
        ]
