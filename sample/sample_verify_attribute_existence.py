#!/usr/bin/env python
# encoding: utf-8


from simplesqlite import SimpleSQLite, TableNotFoundError, AttributeNotFoundError
import six


table_name = "sample_table"
con = SimpleSQLite("sample.sqlite", "w")
con.create_table_with_data(
    table_name=table_name,
    attribute_name_list=["attr_a", "attr_b"],
    data_matrix=[[1, "a"], [2, "b"]])

con.verify_attribute_existence(table_name, "attr_a")
try:
    con.verify_attribute_existence(table_name, "not_existing")
except AttributeNotFoundError as e:
    six.print_(e)
try:
    con.verify_attribute_existence("not_existing", "attr_a")
except TableNotFoundError as e:
    six.print_(e)
