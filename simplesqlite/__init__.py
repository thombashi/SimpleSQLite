# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import

import dataproperty

import simplesqlite.loader
from .core import SimpleSQLite
from ._error import NullDatabaseConnectionError
from ._error import TableNotFoundError
from ._error import AttributeNotFoundError
from ._error import InvalidTableNameError
from ._error import InvalidAttributeNameError
from ._error import SqlSyntaxError
from ._error import OperationalError

from ._func import validate_table_name
from ._func import validate_attr_name
from ._func import append_table
from ._func import connect_sqlite_db_mem
