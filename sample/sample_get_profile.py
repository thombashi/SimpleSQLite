#!/usr/bin/env python
# encoding: utf-8


from simplesqlite import SimpleSQLite
import six


con = SimpleSQLite("sample.sqlite", "w", profile=True)
data_matrix = [
    [1, 1.1, "aaa", 1,   1],
    [2, 2.2, "bbb", 2.2, 2.2],
    [3, 3.3, "ccc", 3,   "ccc"],
]
con.create_table_with_data(
    table_name="sample_table",
    attribute_name_list=["a", "b", "c", "d", "e"],
    data_matrix=data_matrix,
    index_attribute_list=["a"])

for profile in con.get_profile():
    six.print_(profile)
