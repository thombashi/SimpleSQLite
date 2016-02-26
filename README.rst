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

Installation
============

::

    pip install SimpleSQLite

Usage
=====

Create table
------------

.. code:: python

    from simplesqlite import SimpleSQLite

    con = SimpleSQLite("sample.sqlite")

    # create table -----
    data_matrix = [
        [1, 1.1, "aaa", 1,   1],
        [2, 2.2, "bbb", 2.2, 2.2],
        [3, 3.3, "ccc", 3,   "ccc"],
    ]

    con.create_table_with_data(
        table_name="sample_table",
        attribute_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
        data_matrix=data_matrix,
        index_attribute_list=["attr_a"]
    )

    # display values -----
    result = con.select(select="*", table="sample_table")
    for record in result.fetchall():
        print record

    # display type for each column -----
    query = "SELECT DISTINCT TYPEOF(attr_a),TYPEOF(attr_b),TYPEOF(attr_c),TYPEOF(attr_d),TYPEOF(attr_e) FROM sample_table"
    result = con.execute_query(query)
    print result.fetchall()

.. code:: console

    (1, 1.1, u'aaa', 1.0, u'1')
    (2, 2.2, u'bbb', 2.2, u'2.2')
    (3, 3.3, u'ccc', 3.0, u'ccc')
    [(u'integer', u'real', u'text', u'real', u'text')]

insert
------

Dictionary
~~~~~~~~~~

.. code:: python

    con.insert(
        "sample_table",
        {
            "attr_a": 4,
            "attr_b": 4.4,
            "attr_c": "ddd",
            "attr_d": 4.44,
            "attr_e": "hoge",
        }
    )
    con.insert_many(
        "sample_table",
        [
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
    result = con.select(select="*", table="sample_table")
    for record in result.fetchall():
        print record

.. code:: console

    (1, 1.1, u'aaa', 1.0, u'1')
    (2, 2.2, u'bbb', 2.2, u'2.2')
    (3, 3.3, u'ccc', 3.0, u'ccc')
    (4, 4.4, u'ddd', 4.44, u'hoge')
    (5, 5.5, u'eee', 5.55, u'foo')
    (6, u'NULL', u'fff', u'NULL', u'NULL')

list/tuple/namedtuple
~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from collections import namedtuple

    SampleTuple = namedtuple(
        "SampleTuple", "attr_a attr_b attr_c attr_d attr_e")

    con.insert("sample_table", [7, 7.7, "fff", 7.77, "bar"])
    con.insert_many(
        "sample_table",
        [
            (8, 8.8, "ggg", 8.88, "foobar"),
            SampleTuple(9, 9.9, "ggg", 9.99, "hogehoge"),
        ]
    )

    result = con.select(select="*", table="sample_table")
    for record in result.fetchall():
        print record

.. code:: console

    (1, 1.1, u'aaa', 1.0, u'1')
    (2, 2.2, u'bbb', 2.2, u'2.2')
    (3, 3.3, u'ccc', 3.0, u'ccc')
    (4, 4.4, u'ddd', 4.44, u'hoge')
    (5, 5.5, u'eee', 5.55, u'foo')
    (6, u'NULL', u'fff', u'NULL', u'NULL')
    (7, 7.7, u'fff', 7.77, u'bar')
    (8, 8.8, u'ggg', 8.88, u'foobar')
    (9, 9.9, u'ggg', 9.99, u'hogehoge')

Documentation
=============

http://simplesqlite.readthedocs.org/en/latest/apis/simplesqlite.html

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
