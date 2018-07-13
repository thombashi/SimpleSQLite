# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

from textwrap import dedent

import pathvalidate

from .error import NameValidationError


def validate_table_name(name):
    """
    :param str name: Table name to validate.
    :raises NameValidationError: |raises_validate_table_name|
    """

    try:
        pathvalidate.validate_sqlite_table_name(name)
    except pathvalidate.InvalidReservedNameError as e:
        raise NameValidationError(e)
    except pathvalidate.NullNameError:
        raise NameValidationError("table name is empty")
    except pathvalidate.InvalidCharError as e:
        raise NameValidationError(e)
    except pathvalidate.ValidReservedNameError:
        pass


def validate_attr_name(name):
    """
    :param str name: Name to validate.
    :raises NameValidationError: |raises_validate_attr_name|
    """

    try:
        pathvalidate.validate_sqlite_attr_name(name)
    except pathvalidate.InvalidReservedNameError as e:
        raise NameValidationError(e)
    except pathvalidate.NullNameError:
        raise NameValidationError("attribute name is empty")
    except pathvalidate.InvalidCharError as e:
        raise NameValidationError(e)


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
    :raises ValueError:
        If attributes of the table are different from each other.
    """

    src_con.verify_table_existence(table_name)
    dst_con.validate_access_permission(["w", "a"])

    if dst_con.has_table(table_name):
        src_attr_list = src_con.fetch_attr_name_list(table_name)
        dst_attr_list = dst_con.fetch_attr_name_list(table_name)
        if src_attr_list != dst_attr_list:
            raise ValueError(
                dedent(
                    """
                source and destination attribute is different from each other
                src: {}
                dst: {}
                """.format(
                        src_attr_list, dst_attr_list
                    )
                )
            )

    dst_con.create_table_from_tabledata(src_con.select_as_tabledata(table_name))

    return True


def copy_table(src_con, dst_con, src_table_name, dst_table_name, is_overwrite=True):
    """
    Copy a table from source to destination.

    :param SimpleSQLite src_con: Connection to the source database.
    :param SimpleSQLite dst_con: Connection to the destination database.
    :param str src_table_name: Source table name to copy.
    :param str dst_table_name: Destination table name.
    :param bool is_overwrite: If |True|, overwrite existing table.
    :return: |True| if the copy operation succeed.
    :rtype: bool
    :raises simplesqlite.TableNotFoundError:
        |raises_verify_table_existence|
    :raises ValueError:
        If attributes of the table are different from each other.
    """

    from ._logger import logger

    src_con.verify_table_existence(src_table_name)
    dst_con.validate_access_permission(["w", "a"])

    if dst_con.has_table(dst_table_name):
        if is_overwrite:
            dst_con.drop_table(dst_table_name)
        else:
            logger.error(
                "failed to copy table: the table already exists "
                "(src_table={}, dst_table={})".format(src_table_name, dst_table_name)
            )
            return False

    result = src_con.select(select="*", table_name=src_table_name)
    if result is None:
        return False

    dst_con.create_table_from_data_matrix(
        dst_table_name, src_con.fetch_attr_name_list(src_table_name), result.fetchall()
    )

    return True
