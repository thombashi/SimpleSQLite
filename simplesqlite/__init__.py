# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
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
    validate_table_name,
    validate_attr_name,
    append_table,
    connect_sqlite_db_mem,
)
from ._logger import (
    set_logger,
    set_log_level,
)
from .core import SimpleSQLite
