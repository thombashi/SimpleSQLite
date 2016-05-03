#!/usr/bin/env python
# encoding: utf-8


from simplesqlite import SimpleSQLite
import six


table_name = "sample_data"
con = SimpleSQLite("sample.sqlite", "w")
con.create_table_from_csv(csv_path="sample_data.csv")

six.print_(con.get_attribute_name_list(table_name))
result = con.select(select="*", table_name=table_name)
for record in result.fetchall():
    six.print_(record)
