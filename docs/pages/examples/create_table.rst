Create table
--------------

Create a table from data matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`~simplesqlite.SimpleSQLite.create_table_from_data_matrix`
method create a table in a SQLite database from data matrix.
Data matrix required one of the types: |dict|/|namedtuple|/|list|/|tuple|.

.. include:: create_table_from_data_matrix.txt


Create a table from CSV
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`~simplesqlite.SimpleSQLite.create_table_from_csv`
method create a table from a :abbr:`CSV(Comma Separated Values)` file/text.

.. include:: create_table_from_csv.txt


Create table(s) from JSON file/text
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`~simplesqlite.SimpleSQLite.create_table_from_json` method
can create a table from a JSON file/text.

.. include:: create_table_from_json.txt


Create table(s) from an Excel file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:class:`~simplesqlite.loader.ExcelTableFileLoader` class
and :py:meth:`~simplesqlite.SimpleSQLite.create_table_from_tabledata` method
can create a table from an Excel file.

.. include:: create_table_from_excel.txt


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
