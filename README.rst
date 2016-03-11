**SimpleSQLite**

.. image:: https://img.shields.io/pypi/pyversions/SimpleSQLite.svg
   :target: https://pypi.python.org/pypi/SimpleSQLite
.. image:: https://travis-ci.org/thombashi/SimpleSQLite.svg?branch=master
    :target: https://travis-ci.org/thombashi/SimpleSQLite
.. image:: https://ci.appveyor.com/api/projects/status/b564t8y34lkcd1hq/branch/master?svg=true
    :target: https://ci.appveyor.com/project/thombashi/simplesqlite/branch/master
.. image:: https://coveralls.io/repos/github/thombashi/SimpleSQLite/badge.svg?branch=master
    :target: https://coveralls.io/github/thombashi/SimpleSQLite?branch=master

.. contents:: Table of contents
   :backlinks: top
   :local:

Summary
=======
SimpleSQLite is a python library to simplify the table creation and data insertion in SQLite database.

Feature
=======

-  Automatic table creation from data
-  Support various data type for insertion : dictionary, namedtuple,
   list and tuple
-  Create table from csv file

Installation
============

::

    pip install SimpleSQLite

Usage
=====

Create table
------------

Create table from data matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from simplesqlite import SimpleSQLite

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
    print(con.get_attribute_name_list("sample_table"))
    result = con.select(select="*", table_name="sample_table")
    for record in result.fetchall():
        print(record)

    # display data type for each column -----
    print(con.get_attribute_type_list(table_name="sample_table"))

.. code:: console

    ['attr_a', 'attr_b', 'attr_c', 'attr_d', 'attr_e']
    (1, 1.1, u'aaa', 1.0, u'1')
    (2, 2.2, u'bbb', 2.2, u'2.2')
    (3, 3.3, u'ccc', 3.0, u'ccc')
    (u'integer', u'real', u'text', u'real', u'text')

Create table from csv file
~~~~~~~~~~~~~~~~~~~~~~~~~~

Input: sample\_data.csv
^^^^^^^^^^^^^^^^^^^^^^^

::

    "attr_a","attr_b","attr_c"
    1,4,"a"
    2,2.1,"bb"
    3,120.9,"ccc"

Example
^^^^^^^

.. code:: python

    from simplesqlite import SimpleSQLite

    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_from_csv(csv_path="sample_data.csv")

    print(con.get_attribute_name_list("sample_data"))
    result = con.select(select="*", table_name="sample_data")
    for record in result.fetchall():
        print(record)

Output of example
^^^^^^^^^^^^^^^^^

.. code:: console

    ['attr_a', 'attr_b', 'attr_c']
    (1, 4.0, u'a')
    (2, 2.1, u'bb')
    (3, 120.9, u'ccc')

Insert records
--------------

Insert dictionary
~~~~~~~~~~~~~~~~~

.. code:: python

    from simplesqlite import SimpleSQLite

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
        print(record)

.. code:: console

    (1, 1.1, u'aaa', 1, 1)
    (4, 4.4, u'ddd', 4.44, u'hoge')
    (5, 5.5, u'eee', 5.55, u'foo')
    (6, u'NULL', u'fff', u'NULL', u'NULL')

Insert list/tuple/namedtuple
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from collections import namedtuple
    from simplesqlite import SimpleSQLite

    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_with_data(
        table_name="sample_table",
        attribute_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
        data_matrix=[[1, 1.1, "aaa", 1,   1]])

    SampleTuple = namedtuple(
        "SampleTuple", "attr_a attr_b attr_c attr_d attr_e")

    con.insert(
        table_name="sample_table",
        insert_record=[7, 7.7, "fff", 7.77, "bar"])
    con.insert_many(
        table_name="sample_table",
        insert_record_list=[
            (8, 8.8, "ggg", 8.88, "foobar"),
            SampleTuple(9, 9.9, "ggg", 9.99, "hogehoge"),
        ]
    )

    result = con.select(select="*", table_name="sample_table")
    for record in result.fetchall():
        print(record)

.. code:: console

    (1, 1.1, u'aaa', 1, 1)
    (7, 7.7, u'fff', 7.77, u'bar')
    (8, 8.8, u'ggg', 8.88, u'foobar')
    (9, 9.9, u'ggg', 9.99, u'hogehoge')

Documentation
=============

http://simplesqlite.readthedocs.org/en/stable/apis/simplesqlite.html

Dependencies
============

Python 2.5+ or 3.3+

-  `DataPropery <https://github.com/thombashi/DataProperty>`__ (Used to
   extract data types)
-  `six <https://pypi.python.org/pypi/six/>`__

Test dependencies
-----------------

-  `pytest <https://pypi.python.org/pypi/pytest>`__
-  `pytest-runner <https://pypi.python.org/pypi/pytest-runner>`__
-  `tox <https://pypi.python.org/pypi/tox>`__
