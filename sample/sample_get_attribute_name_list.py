#!/usr/bin/env python
# encoding: utf-8


from simplesqlite import SimpleSQLite, TableNotFoundError
import six


table_name = "sample_table"
con = SimpleSQLite("sample.sqlite", "w")
con.create_table_with_data(
    table_name=table_name,
    attribute_name_list=["attr_a", "attr_b"],
    data_matrix=[[1, "a"], [2, "b"]])

six.print_(con.get_attribute_name_list(table_name))

try:
    six.print_(con.get_attribute_name_list("not_existing"))
except TableNotFoundError as e:
    six.print_(e)
