#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

from simplesqlite import SimpleSQLite


con = SimpleSQLite("sample.sqlite", "w", profile=True)
data_matrix = [
    [1, 1.1, "aaa", 1,   1],
    [2, 2.2, "bbb", 2.2, 2.2],
    [3, 3.3, "ccc", 3,   "ccc"],
]
con.create_table_from_data_matrix(
    table_name="sample_table",
    attr_name_list=["a", "b", "c", "d", "e"],
    data_matrix=data_matrix,
    index_attr_list=["a"])

for profile in con.get_profile():
    print(profile)
