Create a table
--------------

Create a table from data matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`~simplesqlite.core.SimpleSQLite.create_table_with_data`
method can get create a table from data matrix.
Data matrix is a list of ``dictionary``/``namedtuple``/``list``/``tuple``.

.. include:: create_table_from_data_matrix.rst


Create a table from a csv file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`~simplesqlite.core.SimpleSQLite.create_table_from_csv` method
can create a table from a :abbr:`csv(comma separated values)` file.

.. object:: Input: sample\_data.csv

    .. parsed-literal::
    
        "attr_a","attr_b","attr_c"
        1,4,"a"
        2,2.1,"bb"
        3,120.9,"ccc"

.. object:: Sample code

    .. code:: python

        from simplesqlite import SimpleSQLite
        import six

        table_name = "sample_data"
        con = SimpleSQLite("sample.sqlite", "w")
        con.create_table_from_csv(csv_path="sample_data.csv")

        six.print_(con.get_attribute_name_list(table_name))
        result = con.select(select="*", table_name=table_name)
        for record in result.fetchall():
            six.print_(record)
    
.. object:: Output

    .. code:: console

        ['attr_a', 'attr_b', 'attr_c']
        (1, 4.0, u'a')
        (2, 2.1, u'bb')
        (3, 120.9, u'ccc')
