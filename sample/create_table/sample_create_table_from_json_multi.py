#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

from simplesqlite import SimpleSQLite


file_path = "sample_data_multi.json"

# create sample data file ---
with open(file_path, "w") as f:
    f.write(
        """{
        "table_a" : [
            {"attr_b": 4, "attr_c": "a", "attr_a": 1},
            {"attr_b": 2.1, "attr_c": "bb", "attr_a": 2},
            {"attr_b": 120.9, "attr_c": "ccc", "attr_a": 3}
        ],
        "table_b" : [
            {"a": 1, "b": 4},
            {"a": 2 },
            {"a": 3, "b": 120.9}
        ]
    }"""
    )

# create table ---
con = SimpleSQLite("sample.sqlite", "w")
con.create_table_from_json(file_path)

# output ---
for table_name in con.fetch_table_name_list():
    print("table: " + table_name)
    print(con.fetch_attr_name_list(table_name))
    result = con.select(select="*", table_name=table_name)
    for record in result.fetchall():
        print(record)
    print()
