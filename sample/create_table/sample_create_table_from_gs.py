#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function
import simplesqlite


credentials_file = "sample-xxxxxxxxxxxx.json"

# create table ---
con = simplesqlite.SimpleSQLite("sample.sqlite", "w")

loader = simplesqlite.loader.GoogleSheetsTableLoader(credentials_file)
loader.title = "samplebook"

for tabledata in loader.load():
    con.create_table_from_tabledata(tabledata)

# output ---
for table_name in con.get_table_name_list():
    print("table: " + table_name)
    print(con.get_attribute_name_list(table_name))
    result = con.select(select="*", table_name=table_name)
    for record in result.fetchall():
        print(record)
    print()
