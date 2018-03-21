# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import

from .__version__ import __author__, __copyright__, __email__, __license__, __version__
from ._error import (
    AttributeNotFoundError, DatabaseError, InvalidAttributeNameError, InvalidTableNameError,
    NullDatabaseConnectionError, OperationalError, SqlSyntaxError, TableNotFoundError)
from ._func import append_table, copy_table
from ._logger import set_log_level, set_logger
from .core import SimpleSQLite, connect_sqlite_db_mem, connect_sqlite_memdb
