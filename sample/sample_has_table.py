#!/usr/bin/env python
# encoding: utf-8


from simplesqlite import SimpleSQLite
import six


con = SimpleSQLite("sample.sqlite", "w")
con.create_table_with_data(
    table_name="hoge",
    attribute_name_list=["attr_a", "attr_b"],
    data_matrix=[[1, "a"], [2, "b"]])
six.print_(con.has_table("hoge"))
six.print_(con.has_table("not_existing"))
