Update a record in a table
--------------------------

:py:meth:`~simplesqlite.SimpleSQLite.update`
method can update record(s) in a table.

:Sample Code:
    .. code-block:: python

        from simplesqlite import SimpleSQLite
        from simplesqlite.query import Where


        table_name = "sample_table"
        con = SimpleSQLite("sample.sqlite", "w")

        data_matrix = [
            [1, "aaa"],
            [2, "bbb"],
        ]
        con.create_table_from_data_matrix(
            table_name,
            ["key", "value"],
            data_matrix)

        print("---- before update ----")
        for record in con.select(select="*", table_name=table_name).fetchall():
            print(record)
        print()

        con.update(table_name, set_query="value = 'ccc'", where=Where(key="key", value=1))

        print("---- after update ----")
        for record in con.select(select="*", table_name=table_name).fetchall():
            print(record)

:Output:
    .. code-block:: none

        ---- before update ----
        (1, 'aaa')
        (2, 'bbb')

        ---- after update ----
        (1, 'ccc')
        (2, 'bbb')
