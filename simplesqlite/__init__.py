"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

import simplesqlite.query  # noqa: F401

from .__version__ import __author__, __copyright__, __email__, __license__, __version__
from ._func import append_table, copy_table
from ._logger import set_log_level, set_logger
from ._sanitizer import SQLiteTableDataSanitizer
from .core import SQLITE_SYSTEM_TABLES, SimpleSQLite, connect_memdb
from .error import (
    AttributeNotFoundError,
    DatabaseError,
    NameValidationError,
    NullDatabaseConnectionError,
    OperationalError,
    SqlSyntaxError,
    TableNotFoundError,
)


__all__ = (
    "__author__",
    "__copyright__",
    "__email__",
    "__license__",
    "__version__",
    "AttributeNotFoundError",
    "DatabaseError",
    "NameValidationError",
    "NullDatabaseConnectionError",
    "OperationalError",
    "SqlSyntaxError",
    "TableNotFoundError",
    "SimpleSQLite",
    "SQLITE_SYSTEM_TABLES",
    "SQLiteTableDataSanitizer",
    "append_table",
    "connect_memdb",
    "copy_table",
    "query",
    "set_log_level",
    "set_logger",
)
