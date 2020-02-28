"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

import simplesqlite.query

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
