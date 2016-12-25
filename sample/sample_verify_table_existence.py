#!/usr/bin/env python
# encoding: utf-8

import simplesqlite


table_name = "sample_table"
con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
con.create_table_from_data_matrix(
    table_name,
    attr_name_list=["attr_a", "attr_b"],
    data_matrix=[[1, "a"], [2, "b"]])

con.verify_table_existence(table_name)
try:
    con.verify_table_existence("not_existing")
except simplesqlite.TableNotFoundError as e:
    print(e)
