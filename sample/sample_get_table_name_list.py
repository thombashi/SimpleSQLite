#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

from simplesqlite import SimpleSQLite


con = SimpleSQLite("sample.sqlite", "w")
con.create_table_from_data_matrix(
    table_name="hoge", attr_name_list=["attr_a", "attr_b"], data_matrix=[[1, "a"], [2, "b"]]
)
print(con.fetch_table_name_list())
