# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


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


class SqlSyntaxError(Exception):
    """
    Raised when a SQLite query syntax is invalid.
    """
