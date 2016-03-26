Profiling
---------

:py:meth:`~simplesqlite.core.SimpleSQLite.get_profile`
method can get profile of query execution time.

.. code:: python

    from simplesqlite import SimpleSQLite
    con = SimpleSQLite("sample.sqlite", "w", profile=True)
    data_matrix = [
        [1, 1.1, "aaa", 1,   1],
        [2, 2.2, "bbb", 2.2, 2.2],
        [3, 3.3, "ccc", 3,   "ccc"],
    ]
    con.create_table_with_data(
        table_name="sample_table",
        attribute_name_list=["a", "b", "c", "d", "e"],
        data_matrix=data_matrix,
        index_attribute_list=["a"])
    for profile in con.get_profile():
        print(profile)

.. parsed-literal::

    SqliteProfile(query=u"CREATE INDEX IF NOT EXISTS sample_table_a_index ON sample_table('a')", cumulative_time=0.021904945373535156, count=1)
    SqliteProfile(query=u"CREATE TABLE IF NOT EXISTS 'sample_table' ('a' INTEGER, 'b' REAL, 'c' TEXT, 'd' REAL, 'e' TEXT)", cumulative_time=0.015315055847167969, count=1)
    SqliteProfile(query=u"DROP TABLE IF EXISTS 'sample_table'", cumulative_time=0.011831998825073242, count=1)
    SqliteProfile(query=u"SELECT name FROM sqlite_master WHERE TYPE='table'", cumulative_time=0.0004591941833496094, count=6)
    SqliteProfile(query=u"SELECT * FROM 'sample_table'", cumulative_time=4.220008850097656e-05, count=1)
