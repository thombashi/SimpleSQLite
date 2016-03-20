#!/usr/bin/env python

from simplesqlite import SimpleSQLite
import six


con = SimpleSQLite("sample.sqlite", "w")
con.create_table_with_data(
    table_name="sample_table",
    attribute_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
    data_matrix=[[1, 1.1, "aaa", 1,   1]])

con.insert(
    table_name="sample_table",
    insert_record={
        "attr_a": 4,
        "attr_b": 4.4,
        "attr_c": "ddd",
        "attr_d": 4.44,
        "attr_e": "hoge",
    }
)
con.insert_many(
    table_name="sample_table",
    insert_record_list=[
        {
            "attr_a": 5,
            "attr_b": 5.5,
            "attr_c": "eee",
            "attr_d": 5.55,
            "attr_e": "foo",
        },
        {
            "attr_a": 6,
            "attr_c": "fff",
        },
    ]
)

result = con.select(select="*", table_name="sample_table")
for record in result.fetchall():
    six.print_(record)
