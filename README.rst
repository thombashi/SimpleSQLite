.. contents:: **SimpleSQLite**
   :backlinks: top
   :depth: 2

Summary
=========
`SimpleSQLite <https://github.com/thombashi/SimpleSQLite>`__ is a Python library to simplify SQLite database operations: table creation, data insertion and get data as other data formats. Simple ORM functionality for SQLite.

.. image:: https://badge.fury.io/py/SimpleSQLite.svg
    :target: https://badge.fury.io/py/SimpleSQLite
    :alt: PyPI package version

.. image:: https://img.shields.io/pypi/pyversions/SimpleSQLite.svg
    :target: https://pypi.org/project/SimpleSQLite
    :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/implementation/SimpleSQLite.svg
    :target: https://pypi.org/project/SimpleSQLite
    :alt: Supported Python implementations

.. image:: https://img.shields.io/travis/thombashi/SimpleSQLite/master.svg?label=Linux/macOS%20CI
    :target: https://travis-ci.org/thombashi/SimpleSQLite
    :alt: Linux/macOS CI status

.. image:: https://img.shields.io/appveyor/ci/thombashi/simplesqlite/master.svg?label=Windows%20CI
    :target: https://ci.appveyor.com/project/thombashi/simplesqlite/branch/master
    :alt: Windows CI status

.. image:: https://coveralls.io/repos/github/thombashi/SimpleSQLite/badge.svg?branch=master
    :target: https://coveralls.io/github/thombashi/SimpleSQLite?branch=master
    :alt: Test coverage

.. image:: https://img.shields.io/github/stars/thombashi/SimpleSQLite.svg?style=social&label=Star
    :target: https://github.com/thombashi/SimpleSQLite
    :alt: GitHub stars

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
    - `pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html>`__ instance
    - `tabledata.TableData <https://tabledata.readthedocs.io/en/latest/pages/reference/data.html>`__ instance loaded by `pytablereader <https://github.com/thombashi/pytablereader>`__
- Get data from a table as:
    - `pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html>`__ instance
    - `tabledata.TableData <https://github.com/thombashi/tabledata>`__ instance
- Simple object-relational mapping (ORM) functionality

Examples
==========
Create a table
----------------
Create a table from data matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:Sample Code:
    .. code-block:: python

        from simplesqlite import SimpleSQLite


        table_name = "sample_table"
        con = SimpleSQLite("sample.sqlite", "w")

        # create table -----
        data_matrix = [[1, 1.1, "aaa", 1, 1], [2, 2.2, "bbb", 2.2, 2.2], [3, 3.3, "ccc", 3, "ccc"]]
        con.create_table_from_data_matrix(
            table_name,
            ["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
            data_matrix,
        )

        # display data type for each column in the table -----
        print(con.schema_extractor.fetch_table_schema(table_name).dumps())

        # display values in the table -----
        print("records:")
        result = con.select(select="*", table_name=table_name)
        for record in result.fetchall():
            print(record)

:Output:
    .. code-block::

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
        print(con.fetch_attr_names(table_name))
        result = con.select(select="*", table_name=table_name)
        for record in result.fetchall():
            print(record)

:Output:
    .. code-block::

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
    .. code-block:: sql

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
            ["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
            [[1, 1.1, "aaa", 1,   1]])

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
            records=[
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
    .. code-block::

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
            ["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"],
            [[1, 1.1, "aaa", 1, 1]],
        )

        # insert namedtuple
        SampleTuple = namedtuple("SampleTuple", "attr_a attr_b attr_c attr_d attr_e")

        con.insert(table_name, record=[7, 7.7, "fff", 7.77, "bar"])
        con.insert_many(
            table_name,
            records=[(8, 8.8, "ggg", 8.88, "foobar"), SampleTuple(9, 9.9, "ggg", 9.99, "hogehoge")],
        )

        # print
        result = con.select(select="*", table_name=table_name)
        for record in result.fetchall():
            print(record)

:Output:
    .. code-block::

        (1, 1.1, 'aaa', 1, 1)
        (7, 7.7, 'fff', 7.77, 'bar')
        (8, 8.8, 'ggg', 8.88, 'foobar')
        (9, 9.9, 'ggg', 9.99, 'hogehoge')

Fetch data from a table as pandas DataFrame
---------------------------------------------
:Sample Code:
    .. code-block:: python

        from simplesqlite import SimpleSQLite

        con = SimpleSQLite("sample.sqlite", "w", profile=True)

        con.create_table_from_data_matrix(
            "sample_table",
            ["a", "b", "c", "d", "e"],
            [
                [1, 1.1, "aaa", 1,   1],
                [2, 2.2, "bbb", 2.2, 2.2],
                [3, 3.3, "ccc", 3,   "ccc"],
            ])

        print(con.select_as_dataframe(table_name="sample_table"))

:Output:
    .. code-block::

        $ sample/select_as_dataframe.py
           a    b    c    d    e
        0  1  1.1  aaa  1.0    1
        1  2  2.2  bbb  2.2  2.2
        2  3  3.3  ccc  3.0  ccc

ORM functionality
-------------------
:Sample Code:
    .. code-block:: python

        from simplesqlite import connect_memdb
        from simplesqlite.model import Integer, Model, Real, Text


        class Sample(Model):
            foo_id = Integer(primary_key=True)
            name = Text(not_null=True, unique=True)
            value = Real()


        def main():
            con = connect_memdb()

            Sample.attach(con)
            Sample.create()
            Sample.insert(Sample(name="abc", value=0.1))
            Sample.insert(Sample(name="xyz", value=1.11))
            Sample.insert(Sample(name="bar", value=2.22))

            print(Sample.fetch_schema().dumps())
            print("records:")
            for record in Sample.select():
                print("    {}".format(record))

            return 0


        if __name__ == "__main__":
            sys.exit(main())

:Output:
    .. code-block::

        .. table:: sample

            +---------+-------+-----------+--------+------+-----+
            |Attribute| Type  |PRIMARY KEY|NOT NULL|UNIQUE|Index|
            +=========+=======+===========+========+======+=====+
            |foo_id   |INTEGER|X          |        |      |     |
            +---------+-------+-----------+--------+------+-----+
            |name     |TEXT   |           |X       |X     |     |
            +---------+-------+-----------+--------+------+-----+
            |value    |REAL   |           |        |      |     |
            +---------+-------+-----------+--------+------+-----+


        records:
            Sample: foo_id=1, name=abc, value=0.1
            Sample: foo_id=2, name=xyz, value=1.11
            Sample: foo_id=3, name=bar, value=2.22

For more information
----------------------
More examples are available at 
https://simplesqlite.rtfd.io/en/latest/pages/examples/index.html

Installation
============
Install from PyPI
------------------------------
::

    pip install SimpleSQLite

Install from PPA (for Ubuntu)
------------------------------
::

    sudo add-apt-repository ppa:thombashi/ppa
    sudo apt update
    sudo apt install python3-simplesqlite


Dependencies
============
Python 2.7+ or 3.5+

Mandatory Dependencies
----------------------------------
- `DataProperty <https://github.com/thombashi/DataProperty>`__ (Used to extract data types)
- `mbstrdecoder <https://github.com/thombashi/mbstrdecoder>`__
- `pathvalidate <https://github.com/thombashi/pathvalidate>`__
- `six <https://pypi.org/project/six/>`__
- `sqliteschema <https://github.com/thombashi/sqliteschema>`__
- `tabledata <https://github.com/thombashi/tabledata>`__
- `typepy <https://github.com/thombashi/typepy>`__

Optional Dependencies
----------------------------------
- `loguru <https://github.com/Delgan/loguru>`__
    - Used for logging if the package installed
- `pandas <https://pandas.pydata.org/>`__
- `pytablereader <https://github.com/thombashi/pytablereader>`__

Test Dependencies
----------------------------------
- `pytest <https://docs.pytest.org/en/latest/>`__
- `tox <https://testrun.org/tox/latest/>`__

Documentation
===============
https://simplesqlite.rtfd.io/

Related Project
=================
- `sqlitebiter <https://github.com/thombashi/sqlitebiter>`__: CLI tool to convert CSV/Excel/HTML/JSON/LTSV/Markdown/TSV/Google-Sheets SQLite database by using SimpleSQLite

