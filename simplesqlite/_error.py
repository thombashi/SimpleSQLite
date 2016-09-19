# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

import sqlite3


class NullDatabaseConnectionError(Exception):
    """
    Raised when executing an operation of
    :py:class:`~simplesqlite.SimpleSQLite` instance without connection to
    a SQLite database file.
    """


class TableNotFoundError(Exception):
    """
    Raised when accessed the table that not exists in the database.
    """


class AttributeNotFoundError(Exception):
    """
    Raised when accessed the attribute that not exists in the table.
    """


class InvalidTableNameError(ValueError):
    """
    Raised when used invalid table name for SQLite.
    """


class InvalidAttributeNameError(ValueError):
    """
    Raised when used invalid attribute name for SQLite.
    """


class SqlSyntaxError(Exception):
    """
    Raised when a SQLite query syntax is invalid.
    """


class OperationalError(sqlite3.OperationalError):
    """
    Raised when failed to execute a query
    """
