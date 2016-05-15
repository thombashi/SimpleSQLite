Insert dictionary
~~~~~~~~~~~~~~~~~

.. code-block:: python
    :caption: Sample code
    
    from simplesqlite import SimpleSQLite
    import six

    table_name = "sample_table"
    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_with_data(
        table_name,
        attribute_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
        data_matrix=[[1, 1.1, "aaa", 1,   1]])

    con.insert(
        table_name,
        insert_record={
            "attr_a": 4,
            "attr_b": 4.4,
            "attr_c": "ddd",
            "attr_d": 4.44,
            "attr_e": "hoge",
        }
    )
    con.insert_many(
        table_name,
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

    result = con.select(select="*", table_name=table_name)
    for record in result.fetchall():
        six.print_(record)

.. code-block:: none
    :caption: Output

    (1, 1.1, u'aaa', 1, 1)
    (4, 4.4, u'ddd', 4.44, u'hoge')
    (5, 5.5, u'eee', 5.55, u'foo')
    (6, u'NULL', u'fff', u'NULL', u'NULL')


Insert list/tuple/namedtuple
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python
    :caption: Sample code
    
    from collections import namedtuple
    from simplesqlite import SimpleSQLite
    import six

    table_name = "sample_table"
    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_with_data(
        table_name,
        attribute_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
        data_matrix=[[1, 1.1, "aaa", 1,   1]])

    SampleTuple = namedtuple(
        "SampleTuple", "attr_a attr_b attr_c attr_d attr_e")

    con.insert(table_name, insert_record=[7, 7.7, "fff", 7.77, "bar"])
    con.insert_many(
        table_name,
        insert_record_list=[
            (8, 8.8, "ggg", 8.88, "foobar"),
            SampleTuple(9, 9.9, "ggg", 9.99, "hogehoge"),
        ]
    )

    result = con.select(select="*", table_name=table_name)
    for record in result.fetchall():
        six.print_(record)


.. code-block:: none
    :caption: Output

    (1, 1.1, u'aaa', 1, 1)
    (7, 7.7, u'fff', 7.77, u'bar')
    (8, 8.8, u'ggg', 8.88, u'foobar')
    (9, 9.9, u'ggg', 9.99, u'hogehoge')