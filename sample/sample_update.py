#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

from simplesqlite import SimpleSQLite
from simplesqlite.query import Where


table_name = "sample_table"
con = SimpleSQLite("sample.sqlite", "w")

data_matrix = [[1, "aaa"], [2, "bbb"]]
con.create_table_from_data_matrix(
    table_name, attr_name_list=["key", "value"], data_matrix=data_matrix
)

print("---- before update ----")
for record in con.select(select="*", table_name=table_name).fetchall():
    print(record)
print()

con.update(table_name, set_query="value = 'ccc'", where=Where(key="key", value=1))

print("---- after update ----")
for record in con.select(select="*", table_name=table_name).fetchall():
    print(record)
