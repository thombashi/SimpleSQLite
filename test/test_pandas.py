# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import unicode_literals

import pytest
from simplesqlite import connect_memdb

from ._common import print_test_result


try:
    import pandas

    PANDAS_IMPORT = True
except ImportError:
    PANDAS_IMPORT = False


@pytest.mark.skipif("PANDAS_IMPORT is False")
class Test_fromto_pandas_dataframe(object):
    def test_normal(self):
        con = connect_memdb()
        column_list = ["id", "value", "name"]
        dataframe = pandas.DataFrame(
            [[0, 0.1, "a"], [1, 1.1, "bb"], [2, 2.2, "ccc"]], columns=column_list
        )
        table_name = "tablename"

        con.create_table_from_dataframe(dataframe, table_name)

        actual_all = con.select_as_dataframe(table_name=table_name)
        print_test_result(expected=dataframe, actual=actual_all)

        assert actual_all.equals(dataframe)

        select_column_list = ["value", "name"]
        actual_part = con.select_as_dataframe(table_name=table_name, column_list=select_column_list)
        assert actual_part.equals(
            pandas.DataFrame([[0.1, "a"], [1.1, "bb"], [2.2, "ccc"]], columns=select_column_list)
        )
