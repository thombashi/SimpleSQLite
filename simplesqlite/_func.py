# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import pathvalidate

from ._error import InvalidAttributeNameError
from ._error import InvalidTableNameError


MEMORY_DB_NAME = ":memory:"


def validate_table_name(name):
    """
    :param str name: Table name to validate.
    :raises InvalidTableNameError: |raises_validate_table_name|
    """

    try:
        pathvalidate.validate_sqlite_table_name(name)
    except pathvalidate.InvalidReservedNameError as e:
        raise InvalidTableNameError(e)
    except pathvalidate.NullNameError:
        raise InvalidTableNameError("table name is empty")
    except pathvalidate.InvalidCharError as e:
        raise InvalidTableNameError(e)


def validate_attr_name(name):
    """
    :param str name: Name to validate.
    :raises InvalidAttributeNameError: |raises_validate_attr_name|
    """

    try:
        pathvalidate.validate_sqlite_attr_name(name)
    except pathvalidate.InvalidReservedNameError as e:
        raise InvalidAttributeNameError(e)
    except pathvalidate.NullNameError:
        raise InvalidAttributeNameError("attribute name is empty")
    except pathvalidate.InvalidCharError as e:
        raise InvalidAttributeNameError(e)


def append_table(src_con, dst_con, table_name):
    """
    Append a table from source database to destination database.

    :param SimpleSQLite src_con: Connection to the source database.
    :param SimpleSQLite dst_con: Connection to the destination database.
    :param str table_name: Table name to append.
    :return: |True| if the append operation succeed.
    :rtype: bool
    :raises simplesqlite.TableNotFoundError:
        |raises_verify_table_existence|
    :raises ValueError: If attribute of the table is different from each other.
    """

    src_con.verify_table_existence(table_name)
    dst_con.validate_access_permission(["w", "a"])

    if dst_con.has_table(table_name):
        src_attr_list = src_con.get_attr_name_list(table_name)
        dst_attr_list = dst_con.get_attr_name_list(table_name)
        if src_attr_list != dst_attr_list:
            raise ValueError("""
            source and destination attribute is different from each other
              src: {:s}
              dst: {:s}
            """.format(str(src_attr_list), str(dst_attr_list)))

    result = src_con.select(select="*", table_name=table_name)
    if result is None:
        return False

    dst_con.create_table_from_data_matrix(
        table_name,
        src_con.get_attr_name_list(table_name),
        result.fetchall())

    return True


def connect_sqlite_db_mem():
    """
    :return: Instance of a in memory database.
    :rtype: SimpleSQLite

    :Examples:

        :ref:`example-connect-sqlite-db-mem`
    """

    from .core import SimpleSQLite

    return SimpleSQLite(MEMORY_DB_NAME, "w")
