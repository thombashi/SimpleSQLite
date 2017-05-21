#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

from simplesqlite import SimpleSQLite
import pandas


con = SimpleSQLite("pandas_df.sqlite")

con.create_table_from_dataframe(pandas.DataFrame(
    [
        [0, 0.1, "a"],
        [1, 1.1, "bb"],
        [2, 2.2, "ccc"],
    ],
    columns=['id', 'value', 'name']
), table_name="pandas_df")
