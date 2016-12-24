#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

import simplesqlite


table_name = "sample_table"
con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
con.create_table_from_data_matrix(
    table_name=table_name,
    attr_name_list=["attr_a", "attr_b"],
    data_matrix=[[1, "a"], [2, "b"]])

print(con.has_attribute_list(table_name, ["attr_a"]))
print(con.has_attribute_list(table_name, ["attr_a", "attr_b"]))
print(con.has_attribute_list(
    table_name, ["attr_a", "attr_b", "not_existing"]))
try:
    print(con.has_attribute("not_existing", ["attr_a"]))
except simplesqlite.TableNotFoundError as e:
    print(e)
