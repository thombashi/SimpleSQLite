"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from collections.abc import Sequence
from typing import Optional, Union

import typepy

from ._func import validate_table_name
from .query import And, Attr, Or, Set, Table, Value, Where, WhereQuery


class SqlQuery:
    """
    Support class for making SQLite query.
    """

    @classmethod
    def make_update(
        cls, table: str, set_query: Union[str, Sequence[Set]], where: Optional[WhereQuery] = None
    ) -> str:
        """
        Make UPDATE query.

        :param str table: Table name of executing the query.
        :param str set_query: SET part of the UPDATE query.
        :param str where:
            Add a WHERE clause to execute query,
            if the value is not |None|.
        :return: Query of SQLite.
        :rtype: str
        :raises ValueError: If ``set_query`` is empty string.
        :raises simplesqlite.NameValidationError:
            |raises_validate_table_name|
        """

        if isinstance(set_query, str):
            norm_set_query = set_query
        else:
            norm_set_query = ", ".join([query.to_query() for query in set_query])

        validate_table_name(table)
        if typepy.is_null_string(norm_set_query):
            raise ValueError("SET query is null")

        query_list = [f"UPDATE {Table(table):s}", f"SET {norm_set_query:s}"]
        if where and isinstance(where, (str, Where, And, Or)):
            query_list.append(f"WHERE {where:s}")

        return " ".join(query_list)

    @classmethod
    def make_where_in(cls, key: str, value_list: Sequence[str]) -> str:
        """
        Make part of WHERE IN query.

        :param str key: Attribute name of the key.
        :param Sequence[str] value_list:
            List of values that the right hand side associated with the key.
        :return: Part of WHERE query of SQLite.
        :rtype: str

        :Examples:
            >>> from simplesqlite.sqlquery import SqlQuery
            >>> SqlQuery.make_where_in("key", ["hoge", "foo", "bar"])
            "key IN ('hoge', 'foo', 'bar')"
        """

        return "{:s} IN ({:s})".format(
            Attr(key), ", ".join([Value(value).to_query() for value in value_list])
        )

    @classmethod
    def make_where_not_in(cls, key: str, value_list: Sequence[str]) -> str:
        """
        Make part of WHERE NOT IN query.

        :param str key: Attribute name of the key.
        :param Sequence[str] value_list:
            List of values that the right hand side associated with the key.
        :return: Part of WHERE query of SQLite.
        :rtype: str

        :Example:
            >>> from simplesqlite.sqlquery import SqlQuery
            >>> SqlQuery.make_where_not_in("key", ["hoge", "foo", "bar"])
            "key NOT IN ('hoge', 'foo', 'bar')"
        """

        return "{:s} NOT IN ({:s})".format(
            Attr(key), ", ".join([Value(value).to_query() for value in value_list])
        )
