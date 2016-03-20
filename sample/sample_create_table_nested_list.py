#!/usr/bin/env python

from simplesqlite import SimpleSQLite
import six


con = SimpleSQLite("sample.sqlite")

data_matrix = [
    [1, 1.1, "aaa", 1,   1],
    [2, 2.2, "bbb", 2.2, 2.2],
    [3, 3.3, "ccc", 3,   "ccc"],
]
con.create_table_with_data(
    table_name="sample_table",
    attribute_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
    data_matrix=data_matrix)

# display values -----
six.print_(con.get_attribute_name_list("sample_table"))
result = con.select(select="*", table_name="sample_table")
for record in result.fetchall():
    six.print_(record)

# display data type for each column -----
six.print_(con.get_attribute_type_list(table_name="sample_table"))
