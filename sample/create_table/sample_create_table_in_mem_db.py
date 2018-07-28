#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

import json

import simplesqlite


table_name = "sample_table"
con = simplesqlite.connect_memdb()

# create table -----
data_matrix = [
    [1, 1.1, "aaa", 1,   1],
    [2, 2.2, "bbb", 2.2, 2.2],
    [3, 3.3, "ccc", 3,   "ccc"],
]
con.create_table_from_data_matrix(
    table_name,
    attr_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
    data_matrix=data_matrix)

# display values in the table -----
print(con.fetch_attr_name_list(table_name))
result = con.select(select="*", table_name=table_name)
for record in result.fetchall():
    print(record)

# display data type for each column in the table -----
print(json.dumps(con.fetch_attr_type(table_name), indent=4))
