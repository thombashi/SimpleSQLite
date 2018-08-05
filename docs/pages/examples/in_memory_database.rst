.. _example-connect-sqlite-db-mem:

Make an in-memory database
--------------------------
:py:func:`~simplesqlite.connect_memdb` function can create a SQLite database in memory.
This function return |SimpleSQLite| instance,
the instance can execute methods as well as a |SimpleSQLite| instance opened with write mode.

:Sample Code:
    .. code-block:: python
        :caption: Make an in-memory database and create a table in the database

        import simplesqlite


        table_name = "sample_table"
        con = simplesqlite.connect_memdb()

        # create table -----
        data_matrix = [[1, 1.1, "aaa", 1, 1], [2, 2.2, "bbb", 2.2, 2.2], [3, 3.3, "ccc", 3, "ccc"]]
        con.create_table_from_data_matrix(
            table_name,
            attr_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
            data_matrix=data_matrix,
        )

        # display data type for each column in the table -----
        print(con.schema_extractor.fetch_table_schema(table_name).dumps())

        # display values in the table -----
        print("records:")
        result = con.select(select="*", table_name=table_name)
        for record in result.fetchall():
            print(record)

:Output:
    .. code-block:: none

        .. table:: sample_table

            +---------+-------+-----------+--------+------+-----+
            |Attribute| Type  |PRIMARY KEY|NOT NULL|UNIQUE|Index|
            +=========+=======+===========+========+======+=====+
            |attr_a   |INTEGER|           |        |      |     |
            +---------+-------+-----------+--------+------+-----+
            |attr_b   |REAL   |           |        |      |     |
            +---------+-------+-----------+--------+------+-----+
            |attr_c   |TEXT   |           |        |      |     |
            +---------+-------+-----------+--------+------+-----+
            |attr_d   |REAL   |           |        |      |     |
            +---------+-------+-----------+--------+------+-----+
            |attr_e   |TEXT   |           |        |      |     |
            +---------+-------+-----------+--------+------+-----+


        records:
        (1, 1.1, 'aaa', 1.0, '1')
        (2, 2.2, 'bbb', 2.2, '2.2')
        (3, 3.3, 'ccc', 3.0, 'ccc')
