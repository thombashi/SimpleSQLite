#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

import pytablereader
import simplesqlite
import xlsxwriter


file_path = "sample_data.xlsx"

# create sample data file ---
workbook = xlsxwriter.Workbook(file_path)

worksheet = workbook.add_worksheet("samplesheet1")
table = [
    ["", "", "", ""],
    ["", "a", "b", "c"],
    ["", 1, 1.1, "a"],
    ["", 2, 2.2, "bb"],
    ["", 3, 3.3, "cc"],
]
for row_idx, row in enumerate(table):
    for col_idx, item in enumerate(row):
        worksheet.write(row_idx, col_idx, item)

worksheet = workbook.add_worksheet("samplesheet2")

worksheet = workbook.add_worksheet("samplesheet3")
table = [
    ["", "", ""],
    ["", "", ""],
    ["aa", "ab", "ac"],
    [1, "hoge", "a"],
    [2, "", "bb"],
    [3, "foo", ""],
]
for row_idx, row in enumerate(table):
    for col_idx, item in enumerate(row):
        worksheet.write(row_idx, col_idx, item)

workbook.close()

# create table ---
con = simplesqlite.SimpleSQLite("sample.sqlite", "w")

loader = pytablereader.ExcelTableFileLoader(file_path)
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
