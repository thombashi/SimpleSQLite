# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

import sqlite3


class DatabaseError(sqlite3.DatabaseError):
    """
    Exception raised for errors that are related to the database.

    .. seealso::

        - `sqlite3.DatabaseError <https://docs.python.org/3/library/sqlite3.html#sqlite3.DatabaseError>`__
    """


class NullDatabaseConnectionError(Exception):
    """
    Exception raised when executing an operation of
    :py:class:`~simplesqlite.SimpleSQLite` instance without connection to
    a SQLite database file.
    """


class TableNotFoundError(Exception):
    """
    Exception raised when accessed the table that not exists in the database.
    """


class AttributeNotFoundError(Exception):
    """
    Exception raised when accessed the attribute that not exists in the table.
    """


class InvalidTableNameError(ValueError):
    """
    Exception raised when used invalid table name for SQLite.
    """


class InvalidAttributeNameError(ValueError):
    """
    Exception raised when used invalid attribute name for SQLite.
    """


class SqlSyntaxError(Exception):
    """
    Exception raised when a SQLite query syntax is invalid.
    """


class OperationalError(sqlite3.OperationalError):
    """
    Exception raised when failed to execute a query.
    """
