#!/usr/bin/env python
# encoding: utf-8


from simplesqlite import SimpleSQLite
from simplesqlite.sqlquery import SqlQuery
import six


table_name = "sample_table"
con = SimpleSQLite("sample.sqlite", "w")

data_matrix = [
    [1, "aaa"],
    [2, "bbb"],
]
con.create_table_with_data(
    table_name,
    attribute_name_list=["key", "value"],
    data_matrix=data_matrix)

six.print_("---- before update ----")
for record in con.select(select="*", table_name=table_name).fetchall():
    six.print_(record)
six.print_()

con.update(
    table_name,
    set_query="value = 'ccc'",
    where=SqlQuery.make_where(key="key", value=1))

six.print_("---- after update ----")
for record in con.select(select="*", table_name=table_name).fetchall():
    six.print_(record)
