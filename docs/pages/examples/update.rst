Update a record in a table
--------------------------

:py:meth:`~simplesqlite.SimpleSQLite.update`
method can update record(s) in a table.

.. code-block:: python
    :caption: Sample code
    
    from simplesqlite import SimpleSQLite
    from simplesqlite.sqlquery import SqlQuery


    table_name = "sample_table"
    con = SimpleSQLite("sample.sqlite", "w")

    data_matrix = [
        [1, "aaa"],
        [2, "bbb"],
    ]
    con.create_table_from_data_matrix(
        table_name,
        attr_name_list=["key", "value"],
        data_matrix=data_matrix)

    print("---- before update ----")
    for record in con.select(select="*", table_name=table_name).fetchall():
        print(record)
    print()

    con.update(
        table_name,
        set_query="value = 'ccc'",
        where=SqlQuery.make_where(key="key", value=1))

    print("---- after update ----")
    for record in con.select(select="*", table_name=table_name).fetchall():
        print(record)


.. code-block:: none
    :caption: Output

    ---- before update ----
    (1, u'aaa')
    (2, u'bbb')

    ---- after update ----
    (1, u'ccc')
    (2, u'bbb')
