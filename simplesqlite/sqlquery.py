# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

import re

import typepy

from ._func import validate_table_name
from .query import Attr, Table, Value


class SqlQuery(object):
    """
    Support class for making SQLite query.
    """

    __RE_SANITIZE = re.compile("[{:s}]".format(re.escape("%/()[]<>.:;'\"!\# -+=\n\r")))
    __RE_SANITIZE_ATTR = re.compile("[{:s}]".format(re.escape("'\"")))
    __RE_TABLE_STR = re.compile("[{:s}]".format(re.escape("%()-+/.")))
    __RE_TO_ATTR_QUOTE = re.compile("[{:s}]".format(re.escape("[_]")))
    __RE_TO_ATTR_BRACKET = re.compile("[{:s}0-9\s#]".format(re.escape("%()-+/.'\"")))
    __RE_SPACE = re.compile("[\s]+")

    __VALID_WHERE_OPERATION_LIST = ["=", "==", "!=", "<>", ">", ">=", "<", "<="]

    @classmethod
    def sanitize(cls, query_item):
        """[Deprecated]
        Replace invalid characters in a SQLite query with empty characters.

        :param str query_item: String to be sanitized.
        :return:
            String that exclude invalid chars.
            Invalid operators are as follows:
            ``"%"``, ``"/"``, ``"("``, ``")"``, ``"["``, ``"]"``,
            ``"<"``, ``">"``, ``"."``, ``":"``, ``";"``, ``"'"``,
            ``"!"``, ``"\"``, ``"#"``, ``"-"``, ``"+"``, ``"="``,
            ``"\\n"``, ``"\\r"``
        :rtype: str

        :Example:
            >>> from simplesqlite.sqlquery import SqlQuery
            >>> SqlQuery.sanitize("k<e:y")
            'key'
        """

        return cls.__RE_SANITIZE.sub("", query_item)

    @classmethod
    def make_insert(cls, table, insert_tuple):
        """
        Make INSERT query.

        :param str table: Table name of executing the query.
        :param list/tuple insert_tuple: Insertion data.
        :return: Query of SQLite.
        :rtype: str
        :raises ValueError: If ``insert_tuple`` is empty |list|/|tuple|.
        :raises simplesqlite.NameValidationError:
            |raises_validate_table_name|
        """

        validate_table_name(table)

        table = Table(table)

        if typepy.is_empty_sequence(insert_tuple):
            raise ValueError("empty insert list/tuple")

        return "INSERT INTO {:s} VALUES ({:s})".format(
            table, ",".join(["?" for _i in insert_tuple])
        )

    @classmethod
    def make_update(cls, table, set_query, where=None):
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

        validate_table_name(table)
        if typepy.is_null_string(set_query):
            raise ValueError("SET query is null")

        query_list = ["UPDATE {:s}".format(Table(table)), "SET {:s}".format(set_query)]
        if typepy.is_not_null_string(where):
            query_list.append("WHERE {:s}".format(where))

        return " ".join(query_list)

    @classmethod
    def make_where_in(cls, key, value_list):
        """
        Make part of WHERE IN query.

        :param str key: Attribute name of the key.
        :param str value_list:
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
    def make_where_not_in(cls, key, value_list):
        """
        Make part of WHERE NOT IN query.

        :param str key: Attribute name of the key.
        :param str value_list:
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
