Get Data from a Table
----------------------------

.. _example-select-as-dict:

Get Data from a table as OrderedDict
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:py:meth:`~simplesqlite.SimpleSQLite.select_as_dict`
method can get data from a table in a SQLite database as
a |OrderedDict| list.

.. include:: select_as_dict.txt


.. _example-select-as-dataframe:

Get Data from a table as pandas DataFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:py:meth:`~simplesqlite.SimpleSQLite.select_as_dataframe`
method can get data from a table in a SQLite database as
a :py:class:`pandas.Dataframe` instance.

.. include:: select_as_dataframe.txt
