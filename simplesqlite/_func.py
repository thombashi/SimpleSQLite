"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from textwrap import dedent
from typing import TYPE_CHECKING

from pathvalidate.error import ErrorReason, ValidationError

from ._common import extract_table_metadata
from ._logger import logger
from ._validator import validate_sqlite_attribute_name, validate_sqlite_table_name
from .error import NameValidationError


if TYPE_CHECKING:
    from simplesqlite import SimpleSQLite  # noqa


def validate_table_name(name: str) -> None:
    """
    :param str name: Table name to validate.
    :raises NameValidationError: |raises_validate_table_name|
    """

    try:
        validate_sqlite_table_name(name)
    except ValidationError as e:
        if e.reason == ErrorReason.RESERVED_NAME and e.reusable_name:
            pass
        else:
            raise NameValidationError(e)


def validate_attribute_name(name: str) -> None:
    """
    :param str name: Name to validate.
    :raises NameValidationError: |raises_validate_attr_name|
    """

    try:
        validate_sqlite_attribute_name(name)
    except ValidationError as e:
        raise NameValidationError(e)


def append_table(src_db_con: "SimpleSQLite", dst_db_con: "SimpleSQLite", table_name: str) -> bool:
    """
    Append a table from source database to destination database.

    :param SimpleSQLite src_db_con: Connection to the source database.
    :param SimpleSQLite dst_db_con: Connection to the destination database.
    :param str table_name: Table name to append.
    :return: |True| if the append operation succeed.
    :rtype: bool
    :raises simplesqlite.TableNotFoundError:
        |raises_verify_table_existence|
    :raises ValueError:
        If attributes of the table are different from each other.
    """

    logger.debug(
        "append table: src={src_db}.{src_tbl}, dst={dst_db}.{dst_tbl}".format(
            src_db=src_db_con.database_path,
            src_tbl=table_name,
            dst_db=dst_db_con.database_path,
            dst_tbl=table_name,
        )
    )

    src_db_con.verify_table_existence(table_name)
    dst_db_con.validate_access_permission(["w", "a"])

    if dst_db_con.has_table(table_name):
        src_attrs = src_db_con.fetch_attribute_names(table_name)
        dst_attrs = dst_db_con.fetch_attribute_names(table_name)
        if src_attrs != dst_attrs:
            raise ValueError(
                dedent(
                    """
                    source and destination attribute is different from each other
                    src: {}
                    dst: {}
                    """.format(
                        src_attrs, dst_attrs
                    )
                )
            )

    primary_key, index_attrs, type_hints = extract_table_metadata(src_db_con, table_name)

    dst_db_con.create_table_from_tabledata(
        src_db_con.select_as_tabledata(table_name, type_hints=type_hints),
        primary_key=primary_key,
        index_attrs=index_attrs,
    )

    return True


def copy_table(
    src_db_con: "SimpleSQLite",
    dst_db_con: "SimpleSQLite",
    src_table_name: str,
    dst_table_name: str,
    is_overwrite: bool = True,
) -> bool:
    """
    Copy a table from source to destination.

    :param SimpleSQLite src_db_con: Connection to the source database.
    :param SimpleSQLite dst_db_con: Connection to the destination database.
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

    logger.debug(
        "copy table: src={src_db}.{src_tbl}, dst={dst_db}.{dst_tbl}".format(
            src_db=src_db_con.database_path,
            src_tbl=src_table_name,
            dst_db=dst_db_con.database_path,
            dst_tbl=dst_table_name,
        )
    )

    src_db_con.verify_table_existence(src_table_name)
    dst_db_con.validate_access_permission(["w", "a"])

    if dst_db_con.has_table(dst_table_name):
        if is_overwrite:
            dst_db_con.drop_table(dst_table_name)
        else:
            logger.error(
                "failed to copy table: the table already exists "
                "(src_table={}, dst_table={})".format(src_table_name, dst_table_name)
            )
            return False

    primary_key, index_attrs, _ = extract_table_metadata(src_db_con, src_table_name)

    result = src_db_con.select(select="*", table_name=src_table_name)
    if result is None:
        return False

    dst_db_con.create_table_from_data_matrix(
        dst_table_name,
        src_db_con.fetch_attribute_names(src_table_name),
        result.fetchall(),
        primary_key=primary_key,
        index_attrs=index_attrs,
    )

    return True
