#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

from simplesqlite import AttributeNotFoundError, SimpleSQLite, TableNotFoundError


table_name = "sample_table"
con = SimpleSQLite("sample.sqlite", "w")
con.create_table_from_data_matrix(
    table_name=table_name, attr_name_list=["attr_a", "attr_b"], data_matrix=[[1, "a"], [2, "b"]]
)

con.verify_attr_existence(table_name, "attr_a")
try:
    con.verify_attr_existence(table_name, "not_existing")
except AttributeNotFoundError as e:
    print(e)
try:
    con.verify_attr_existence("not_existing", "attr_a")
except TableNotFoundError as e:
    print(e)
