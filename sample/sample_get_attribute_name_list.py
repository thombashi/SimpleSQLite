#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

import simplesqlite


table_name = "sample_table"
con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
con.create_table_with_data(
    table_name=table_name,
    attribute_name_list=["attr_a", "attr_b"],
    data_matrix=[[1, "a"], [2, "b"]])

print(con.get_attribute_name_list(table_name))

try:
    print(con.get_attribute_name_list("not_existing"))
except simplesqlite.TableNotFoundError as e:
    print(e)
