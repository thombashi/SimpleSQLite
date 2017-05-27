# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import unicode_literals

import pytest
from simplesqlite import connect_sqlite_db_mem
from simplesqlite.sqlquery import SqlQuery


try:
    import pandas
    PANDAS_IMPORT = True
except ImportError:
    PANDAS_IMPORT = False


@pytest.mark.skipif("PANDAS_IMPORT is False")
class Test_fromto_pandas_dataframe(object):

    def test_normal(self):
        con = connect_sqlite_db_mem()
        column_list = ['id', 'value', 'name']
        table_name = "tablename"

        dataframe = pandas.DataFrame(
            [
                [0, 0.1, "a"],
                [1, 1.1, "bb"],
                [2, 2.2, "ccc"],
            ],
            columns=column_list
        )

        con.create_table_from_dataframe(dataframe, "tablename")
        result = con.select(
            select=", ".join(
                SqlQuery.to_attr_str_list(['id', 'value', 'name'])),
            table_name=table_name)

        assert result.fetchall() == [
            (0, 0.1, u'a'),
            (1, 1.1, u'bb'),
            (2, 2.2, u'ccc'),
        ]

        actual = con.select_as_dataframe(
            column_list=column_list, table_name=table_name)

        print("[expected]\n{}\n".format(dataframe))
        print("[actual]\n{}\n".format(actual))

        assert actual.equals(dataframe)
