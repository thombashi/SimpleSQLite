SimpleSQLite
============

.. image:: https://badge.fury.io/py/SimpleSQLite.svg
    :target: https://badge.fury.io/py/SimpleSQLite

.. image:: https://img.shields.io/pypi/pyversions/SimpleSQLite.svg
    :target: https://pypi.python.org/pypi/SimpleSQLite

.. image:: https://img.shields.io/travis/thombashi/SimpleSQLite/master.svg?label=Linux
    :target: https://travis-ci.org/thombashi/SimpleSQLite
    :alt: Linux CI test status

.. image:: https://img.shields.io/appveyor/ci/thombashi/simplesqlite/master.svg?label=Windows
    :target: https://ci.appveyor.com/project/thombashi/simplesqlite/branch/master

.. image:: https://coveralls.io/repos/github/thombashi/SimpleSQLite/badge.svg?branch=master
    :target: https://coveralls.io/github/thombashi/SimpleSQLite?branch=master

Summary
-------

SimpleSQLite is a python library to simplify the table creation and data insertion in SQLite database.

Feature
-------

- Automatic table creation from data
- Support various data types of record(s) insertion into a table:
    - dictionary
    - namedtuple
    - list
    - tuple
- Create table(s) from:
    - CSV file/text
    - HTML file/text
    - JSON file/text
    - Microsoft Excel :superscript:`TM` file
    - `Google Sheets <https://www.google.com/intl/en_us/sheets/about/>`_

Examples
========

Create a table
--------------

Create a table from data matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import json
    from simplesqlite import SimpleSQLite
    import six

    table_name = "sample_table"
    con = SimpleSQLite("sample.sqlite", "w")

    # create table -----
    data_matrix = [
        [1, 1.1, "aaa", 1,   1],
        [2, 2.2, "bbb", 2.2, 2.2],
        [3, 3.3, "ccc", 3,   "ccc"],
    ]
    con.create_table_with_data(
        table_name,
        attribute_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
        data_matrix=data_matrix)

    # display values in the table -----
    six.print_(con.get_attribute_name_list(table_name))
    result = con.select(select="*", table_name=table_name)
    for record in result.fetchall():
        six.print_(record)

    # display data type for each column in the table -----
    six.print_(json.dumps(con.get_attr_type(table_name), indent=4))


.. code::

    ['attr_a', 'attr_b', 'attr_c', 'attr_d', 'attr_e']
    (1, 1.1, u'aaa', 1.0, u'1')
    (2, 2.2, u'bbb', 2.2, u'2.2')
    (3, 3.3, u'ccc', 3.0, u'ccc')
    {
        "attr_b": " REAL",
        "attr_c": " TEXT",
        "attr_a": " INTEGER",
        "attr_d": " REAL",
        "attr_e": " TEXT"
    }

Insert records into a table
---------------------------

Insert dictionary
~~~~~~~~~~~~~~~~~

.. code:: python

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

.. code::

    (1, 1.1, u'aaa', 1, 1)
    (4, 4.4, u'ddd', 4.44, u'hoge')
    (5, 5.5, u'eee', 5.55, u'foo')
    (6, u'NULL', u'fff', u'NULL', u'NULL')


Insert list/tuple/namedtuple
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

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


.. code::

    (1, 1.1, u'aaa', 1, 1)
    (7, 7.7, u'fff', 7.77, u'bar')
    (8, 8.8, u'ggg', 8.88, u'foobar')
    (9, 9.9, u'ggg', 9.99, u'hogehoge')

For more information
--------------------

More examples are available at 
http://simplesqlite.readthedocs.org/en/latest/pages/examples/index.html

Installation
============

::

    pip install SimpleSQLite


Dependencies
============

Python 2.7+ or 3.3+

Mandatory
-----------------

- `beautifulsoup4 <https://www.crummy.com/software/BeautifulSoup/>`__
- `DataPropery <https://github.com/thombashi/DataProperty>`__ (Used to extract data types)
- `jsonschema <https://github.com/Julian/jsonschema>`__
- `pathvalidate <https://github.com/thombashi/pathvalidate>`__
- `path.py <https://github.com/jaraco/path.py>`__
- `pytablereader <https://github.com/thombashi/pytablereader>`__
- `six <https://pypi.python.org/pypi/six/>`__
- `xlrd <https://github.com/python-excel/xlrd>`__

Optional
-----------------

- `lxml <http://lxml.de/installation.html>`__ (Faster HTML convert if installed)


Test dependencies
-----------------

-  `pytest <http://pytest.org/latest/>`__
-  `pytest-runner <https://pypi.python.org/pypi/pytest-runner>`__
-  `tox <https://testrun.org/tox/latest/>`__
-  `XlsxWriter <http://xlsxwriter.readthedocs.io/>`__

Documentation
=============

http://simplesqlite.readthedocs.org/en/latest/

Related project
===============

- sqlitebiter: CLI tool to create a SQLite database from CSV/JSON/Excel/Google-Sheets by using SimpleSQLite
    - https://github.com/thombashi/sqlitebiter

