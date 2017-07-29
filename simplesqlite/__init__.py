# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import

from ._error import (
    DatabaseError,
    NullDatabaseConnectionError,
    TableNotFoundError,
    AttributeNotFoundError,
    InvalidTableNameError,
    InvalidAttributeNameError,
    SqlSyntaxError,
    OperationalError,
)
from ._func import (
    append_table,
    copy_table,
)
from ._logger import (
    set_logger,
    set_log_level,
)
from .core import (
    connect_sqlite_memdb,
    connect_sqlite_db_mem,
    SimpleSQLite,
)
