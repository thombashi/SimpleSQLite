#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

from simplesqlite import SimpleSQLite


file_path = "sample_data.csv"

with open(file_path, "w") as f:
    f.write("\n".join([
        '"attr_a","attr_b","attr_c"',
        '1,4,"a"',
        '2,2.1,"bb"',
        '3,120.9,"ccc"',
    ]))

# create table ---
con = SimpleSQLite("sample.sqlite", "w")
con.create_table_from_csv(file_path)

# output ---
table_name = "sample_data"
print(con.get_attribute_name_list(table_name))
result = con.select(select="*", table_name=table_name)
for record in result.fetchall():
    print(record)
