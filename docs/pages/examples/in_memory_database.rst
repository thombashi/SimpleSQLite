Make an in-memory database
--------------------------

:py:func:`~simplesqlite.connect_sqlite_db_mem` 
function can create a SQLite database in memory.

.. code:: python

    >>> import simplesqlite
    >>> con = simplesqlite.connect_sqlite_db_mem()
    >>> con.database_path
    ':memory:'
