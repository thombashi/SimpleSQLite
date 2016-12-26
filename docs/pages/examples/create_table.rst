Create table
--------------

.. _example-create-table-from-data-matrix:

Create a table from data matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`~simplesqlite.SimpleSQLite.create_table_from_data_matrix`
method create a table in a SQLite database from data matrix.
Data matrix required one of the types: |dict|/|namedtuple|/|list|/|tuple|.

.. include:: create_table_from_data_matrix.txt


.. _example-create-table-from-csv:

Create a table from CSV
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`~simplesqlite.SimpleSQLite.create_table_from_csv`
method create a table from a :abbr:`CSV(Comma Separated Values)` file/text.

.. include:: create_table_from_csv.txt


.. _example-create-table-from-json:

Create table(s) from JSON
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`~simplesqlite.SimpleSQLite.create_table_from_json` method
can create a table from a JSON file/text.

.. include:: create_table_from_json.txt


Create table(s) from Excel
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can extract tabular data from an Excel file by 
:py:class:`pytablereader.ExcelTableFileLoader` class defined in
`pytablereader <https://github.com/thombashi/pytablereader>`__ module.
And you can create a table from extracted data by using
:py:meth:`~simplesqlite.SimpleSQLite.create_table_from_tabledata` method.

.. include:: create_table_from_excel.txt


.. _example-create-table-from-gs:

Create table(s) from Google Sheets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:class:`~simplesqlite.loader.GoogleSheetsTableLoader` class
and :py:meth:`~simplesqlite.SimpleSQLite.create_table_from_tabledata` method
can create a table from Google Spreadsheet.

    Required packages:

        - `oauth2client <https://github.com/google/oauth2client/>`_
        - `pyOpenSSL <https://pyopenssl.readthedocs.io/en/stable/>`_

    .. seealso::
    
        http://gspread.readthedocs.io/en/latest/oauth2.html

.. include:: create_table_from_gs.txt
