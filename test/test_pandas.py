"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

import pytest

from simplesqlite import connect_memdb

from ._common import print_test_result


try:
    import pandas as pd

    PANDAS_IMPORT = True
except ImportError:
    PANDAS_IMPORT = False


@pytest.mark.skipif(not PANDAS_IMPORT, reason="required package not found")
class Test_fromto_pandas_dataframe:
    def test_normal(self):
        con = connect_memdb()
        dataframe = pd.DataFrame(
            [[0, 0.1, "a"], [1, 1.1, "bb"], [2, 2.2, "ccc"]], columns=["id", "value", "name"]
        )
        table_name = "tablename"

        con.create_table_from_dataframe(dataframe, table_name)

        actual_all = con.select_as_dataframe(table_name=table_name)
        print_test_result(expected=dataframe, actual=actual_all)

        assert actual_all.equals(dataframe)

        select_columns = ["value", "name"]
        actual_part = con.select_as_dataframe(table_name=table_name, columns=select_columns)
        assert actual_part.equals(
            pd.DataFrame([[0.1, "a"], [1.1, "bb"], [2.2, "ccc"]], columns=select_columns)
        )

    def test_normal_include_datetime(self):
        con = connect_memdb()
        table_name = "table_w_datetime"

        dataframe = pd.DataFrame(
            [
                ["2020-03-25 15:24:00-04:00", 0, 0.1, "a"],
                ["2020-03-25 15:25:00-04:00", 1, 1.1, "bb"],
                ["2020-03-25 15:30:00-04:00", 2, 2.2, "ccc"],
            ],
            columns=["timestamp", "id", "value", "name"],
        )
        dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"])
        con.create_table_from_dataframe(dataframe, table_name=table_name)

        actual_all = con.select_as_dataframe(table_name=table_name)
        print_test_result(expected=dataframe, actual=actual_all)
