SimpleSQLite
==============
.. image:: https://badge.fury.io/py/SimpleSQLite.svg
    :target: https://badge.fury.io/py/SimpleSQLite

.. image:: https://img.shields.io/travis/thombashi/SimpleSQLite/master.svg?label=Linux
    :target: https://travis-ci.org/thombashi/SimpleSQLite
    :alt: Linux CI test status

.. image:: https://img.shields.io/appveyor/ci/thombashi/simplesqlite/master.svg?label=Windows
    :target: https://ci.appveyor.com/project/thombashi/simplesqlite/branch/master

.. image:: https://coveralls.io/repos/github/thombashi/SimpleSQLite/badge.svg?branch=master
    :target: https://coveralls.io/github/thombashi/SimpleSQLite?branch=master

.. image:: https://img.shields.io/github/stars/thombashi/SimpleSQLite.svg?style=social&label=Star
   :target: https://github.com/thombashi/SimpleSQLite

.. contents:: Table of Contents
   :depth: 2

Summary
---------
SimpleSQLite is a Python library to simplify SQLite database operations: table creation, data insertion and get data as other data formats.

Features
--------

- Automated SQLite table creation from data
- Support various data types of record(s) insertion into a table:
    - ``dict``
    - ``namedtuple``
    - ``list``
    - ``tuple``
- Create table(s) from:
    - CSV file/text
    - JSON file/text
    - `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html>`__ instance
    - `tabledata.TableData <https://github.com/thombashi/tabledata>`__ instance loaded by `pytablereader <https://github.com/thombashi/pytablereader>`__
- Get data from a table as:
    - `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html>`__ instance
    - `tabledata.TableData <https://github.com/thombashi/tabledata>`__ instance



Examples
==========
Create a table
----------------
Create a table from data matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:Sample Code:
    .. code-block:: python

        import json
        from simplesqlite import SimpleSQLite

        table_name = "sample_table"
        con = SimpleSQLite("sample.sqlite", "w")

        # create table -----
        data_matrix = [
            [1, 1.1, "aaa", 1,   1],
            [2, 2.2, "bbb", 2.2, 2.2],
            [3, 3.3, "ccc", 3,   "ccc"],
        ]
        con.create_table_from_data_matrix(
            table_name,
            attr_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
            data_matrix=data_matrix)

        # display values in the table -----
        print(con.get_attr_name_list(table_name))
        result = con.select(select="*", table_name=table_name)
        for record in result.fetchall():
            print(record)

        # display data type for each column in the table -----
        print(json.dumps(con.get_attr_type(table_name), indent=4))

:Output:
    .. code-block:: none

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

Create a table from CSV
~~~~~~~~~~~~~~~~~~~~~~~~~
:Sample Code:
    .. code-block:: python

        from simplesqlite import SimpleSQLite

        with open("sample_data.csv", "w") as f:
            f.write("\n".join([
                '"attr_a","attr_b","attr_c"',
                '1,4,"a"',
                '2,2.1,"bb"',
                '3,120.9,"ccc"',
            ]))

        # create table ---
        con = SimpleSQLite("sample.sqlite", "w")
        con.create_table_from_csv("sample_data.csv")

        # output ---
        table_name = "sample_data"
        print(con.get_attr_name_list(table_name))
        result = con.select(select="*", table_name=table_name)
        for record in result.fetchall():
            print(record)

:Output:
    .. code-block:: none

        ['attr_a', 'attr_b', 'attr_c']
        (1, 4.0, u'a')
        (2, 2.1, u'bb')
        (3, 120.9, u'ccc')

Create a table from pandas.DataFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:Sample Code:
    .. code-block:: python

        from simplesqlite import SimpleSQLite
        import pandas

        con = SimpleSQLite("pandas_df.sqlite")

        con.create_table_from_dataframe(pandas.DataFrame(
            [
                [0, 0.1, "a"],
                [1, 1.1, "bb"],
                [2, 2.2, "ccc"],
            ],
            columns=['id', 'value', 'name']
        ), table_name="pandas_df")

:Output:
    .. code-block:: none

        $ sqlite3 pandas_df.sqlite
        sqlite> .schema
        CREATE TABLE 'pandas_df' (id INTEGER, value REAL, name TEXT);

Insert records into a table
-----------------------------
Insert dictionary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Sample Code:
    .. code-block:: python

        from simplesqlite import SimpleSQLite

        table_name = "sample_table"
        con = SimpleSQLite("sample.sqlite", "w")
        con.create_table_from_data_matrix(
            table_name,
            attr_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
            data_matrix=[[1, 1.1, "aaa", 1,   1]])

        con.insert(
            table_name,
            record={
                "attr_a": 4,
                "attr_b": 4.4,
                "attr_c": "ddd",
                "attr_d": 4.44,
                "attr_e": "hoge",
            })
        con.insert_many(
            table_name,
            record_list=[
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
            ])

        result = con.select(select="*", table_name=table_name)
        for record in result.fetchall():
            print(record)

:Output:
    .. code-block:: none

        (1, 1.1, 'aaa', 1, 1)
        (4, 4.4, 'ddd', 4.44, 'hoge')
        (5, 5.5, 'eee', 5.55, 'foo')
        (6, None, 'fff', None, None)


Insert list/tuple/namedtuple
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Sample Code:
    .. code-block:: python

        from collections import namedtuple
        from simplesqlite import SimpleSQLite

        table_name = "sample_table"
        con = SimpleSQLite("sample.sqlite", "w")
        con.create_table_from_data_matrix(
            table_name,
            attr_name_list=["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
            data_matrix=[[1, 1.1, "aaa", 1,   1]])

        SampleTuple = namedtuple(
            "SampleTuple", "attr_a attr_b attr_c attr_d attr_e")

        con.insert(table_name, record=[7, 7.7, "fff", 7.77, "bar"])
        con.insert_many(
            table_name,
            record_list=[
                (8, 8.8, "ggg", 8.88, "foobar"),
                SampleTuple(9, 9.9, "ggg", 9.99, "hogehoge"),
            ])

        result = con.select(select="*", table_name=table_name)
        for record in result.fetchall():
            print(record)

:Output:
    .. code-block:: none

        (1, 1.1, u'aaa', 1, 1)
        (7, 7.7, u'fff', 7.77, u'bar')
        (8, 8.8, u'ggg', 8.88, u'foobar')
        (9, 9.9, u'ggg', 9.99, u'hogehoge')

Get Data from a table as pandas DataFrame
-------------------------------------------
:Sample Code:
    .. code-block:: python

        from simplesqlite import SimpleSQLite

        con = SimpleSQLite("sample.sqlite", "w", profile=True)

        con.create_table_from_data_matrix(
            table_name="sample_table",
            attr_name_list=["a", "b", "c", "d", "e"],
            data_matrix=[
                [1, 1.1, "aaa", 1,   1],
                [2, 2.2, "bbb", 2.2, 2.2],
                [3, 3.3, "ccc", 3,   "ccc"],
            ])

        print(con.select_as_dataframe(table_name="sample_table"))

:Output:
    .. code-block:: none

        $ sample/select_as_dataframe.py
           a    b    c    d    e
        0  1  1.1  aaa  1.0    1
        1  2  2.2  bbb  2.2  2.2
        2  3  3.3  ccc  3.0  ccc

For more information
----------------------
More examples are available at 
http://simplesqlite.rtfd.io/en/latest/pages/examples/index.html

Installation
============
::

    pip install SimpleSQLite


Dependencies
============
Python 2.7+ or 3.4+

Mandatory Dependencies
----------------------------------
- `DataPropery <https://github.com/thombashi/DataProperty>`__ (Used to extract data types)
- `logbook <http://logbook.readthedocs.io/en/stable/>`__
- `mbstrdecoder <https://github.com/thombashi/mbstrdecoder>`__
- `pathvalidate <https://github.com/thombashi/pathvalidate>`__
- `six <https://pypi.python.org/pypi/six/>`__
- `tabledata <https://github.com/thombashi/tabledata>`__
- `typepy <https://github.com/thombashi/typepy>`__

Optional Dependencies
----------------------------------
- `pandas <http://pandas.pydata.org/>`__
- `pytablereader <https://github.com/thombashi/pytablereader>`__

Test Dependencies
----------------------------------
- `pytest <http://pytest.org/latest/>`__
- `pytest-runner <https://pypi.python.org/pypi/pytest-runner>`__
- `tox <https://testrun.org/tox/latest/>`__

Documentation
===============
http://simplesqlite.rtfd.io/

Related project
=================
- `sqlitebiter <https://github.com/thombashi/sqlitebiter>`__: CLI tool to convert CSV/Excel/HTML/JSON/LTSV/Markdown/TSV/Google-Sheets SQLite database by using SimpleSQLite

