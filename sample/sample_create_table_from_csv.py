#!/usr/bin/env python

from simplesqlite import SimpleSQLite
import six


con = SimpleSQLite("sample.sqlite", "w")
con.create_table_from_csv(csv_path="sample_data.csv")

six.print_(con.get_attribute_name_list("sample_data"))
result = con.select(select="*", table_name="sample_data")
for record in result.fetchall():
    six.print_(record)
