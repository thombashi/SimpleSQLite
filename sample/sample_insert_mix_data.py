#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

from collections import namedtuple
from simplesqlite import SimpleSQLite


table_name = "sample_table"
con = SimpleSQLite("sample.sqlite", "w")
con.create_table_from_data_matrix(
    table_name,
    attr_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
    data_matrix=[[1, 1.1, "aaa", 1,   1]])

SampleTuple = namedtuple(
    "SampleTuple", "attr_a attr_b attr_c attr_d attr_e")

con.insert(table_name, record=[7, 7.7, "fff", 7.77, "bar"])
con.insert_many(
    table_name,
    record_list=[
        (8, 8.8, "ggg", 8.88, "foobar"),
        SampleTuple(9, 9.9, "ggg", 9.99, "hogehoge"),
    ])

result = con.select(select="*", table_name=table_name)
for record in result.fetchall():
    print(record)
