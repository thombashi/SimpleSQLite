# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

import sqlite3

from tabledata import NameValidationError  # noqa: W0611


class DatabaseError(sqlite3.DatabaseError):
    """
    Exception raised for errors that are related to the database.

    .. seealso::
        - `sqlite3.DatabaseError <https://docs.python.org/3/library/sqlite3.html#sqlite3.DatabaseError>`__
    """


class NullDatabaseConnectionError(DatabaseError):
    """
    Exception raised when executing an operation of
    :py:class:`~simplesqlite.SimpleSQLite` instance without connection to
    a SQLite database file.
    """


class TableNotFoundError(DatabaseError):
    """
    Exception raised when accessed the table that not exists in the database.
    """


class AttributeNotFoundError(DatabaseError):
    """
    Exception raised when accessed the attribute that not exists in the table.
    """


class SqlSyntaxError(Exception):
    """
    Exception raised when a SQLite query syntax is invalid.
    """


class OperationalError(sqlite3.OperationalError):
    """
    Exception raised when failed to execute a query.
    """

    @property
    def message(self):
        return self.__message

    def __init__(self, *args, **kwargs):
        self.__message = kwargs.pop("message", None)

        super(OperationalError, self).__init__(*args, **kwargs)
