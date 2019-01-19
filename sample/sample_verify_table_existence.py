#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

import simplesqlite


table_name = "sample_table"
con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
con.create_table_from_data_matrix(table_name, ["attr_a", "attr_b"], [[1, "a"], [2, "b"]])

con.verify_table_existence(table_name)
try:
    con.verify_table_existence("not_existing")
except simplesqlite.TableNotFoundError as e:
    print(e)
