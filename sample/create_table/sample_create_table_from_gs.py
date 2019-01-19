#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

import pytablereader as ptr
import simplesqlite


credentials_file = "sample-xxxxxxxxxxxx.json"

# create table ---
con = simplesqlite.SimpleSQLite("sample.sqlite", "w")

loader = ptr.GoogleSheetsTableLoader(credentials_file)
loader.title = "samplebook"

for table_data in loader.load():
    con.create_table_from_tabledata(table_data)

# output ---
for table_name in con.fetch_table_names():
    print("table: " + table_name)
    print(con.fetch_attr_names(table_name))
    result = con.select(select="*", table_name=table_name)
    for record in result.fetchall():
        print(record)
    print()
