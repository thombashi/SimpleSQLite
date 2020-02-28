#!/usr/bin/env python3

from simplesqlite import SimpleSQLite


con = SimpleSQLite("sample.sqlite", "w")
con.create_table_from_data_matrix("hoge", ["attr_a", "attr_b"], [[1, "a"], [2, "b"]])

print(con.has_table("hoge"))
print(con.has_table("not_existing"))
