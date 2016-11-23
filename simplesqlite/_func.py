# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import

import pathvalidate

from ._error import InvalidTableNameError
from ._error import InvalidAttributeNameError


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


def append_table(con_src, con_dst, table_name):
    """
    Append the table from source to destination.

    :param SimpleSQLite con_src: Source of the database.
    :param SimpleSQLite con_dst: Destination of the database.
    :param str table_name: Table name to copy.
    :return: |True| if success.
    :rtype: bool
    :raises simplesqlite.TableNotFoundError:
        |raises_verify_table_existence|
    :raises ValueError: If attribute of the table is different from each other.
    """

    con_src.verify_table_existence(table_name)
    con_dst.validate_access_permission(["w", "a"])

    if con_dst.has_table(table_name):
        src_attr_list = con_src.get_attribute_name_list(table_name)
        dst_attr_list = con_dst.get_attribute_name_list(table_name)
        if src_attr_list != dst_attr_list:
            raise ValueError("""
            source and destination attribute is different from each other
              src: {:s}
              dst: {:s}
            """.format(str(src_attr_list), str(dst_attr_list)))

    result = con_src.select(select="*", table_name=table_name)
    if result is None:
        return False
    value_matrix = result.fetchall()

    con_dst.create_table_with_data(
        table_name,
        con_src.get_attribute_name_list(table_name),
        value_matrix)

    return True


def connect_sqlite_db_mem():
    """
    :return: Instance of a in memory database.
    :rtype: SimpleSQLite

    :Examples:

        .. code:: python

            >>> import simplesqlite
            >>> con = simplesqlite.connect_sqlite_db_mem()
            >>> con.database_path
            ':memory:'
    """

    from .core import SimpleSQLite

    return SimpleSQLite(MEMORY_DB_NAME, "w")
