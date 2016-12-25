# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import
from __future__ import unicode_literals
import re

import dataproperty as dp
import pathvalidate as pv
from six.moves import map

from ._error import SqlSyntaxError
from ._func import validate_table_name


class SqlQuery:
    """
    Support class for making SQLite query.
    """

    __RE_SANITIZE = re.compile(
        "[{:s}]".format(re.escape("%/()[]<>.:;'\"!\# -+=\n\r")))
    __RE_SANITIZE_ATTR = re.compile("[{:s}]".format(re.escape("'\"")))
    __RE_TABLE_STR = re.compile("[{:s}]".format(re.escape("%()-+/.")))
    __RE_TO_ATTR_QUOTE = re.compile("[{:s}]".format(re.escape("[_]")))
    __RE_TO_ATTR_BRACKET = re.compile(
        "[{:s}0-9\s#]".format(re.escape("%()-+/.'\"")))
    __RE_SPACE = re.compile("[\s]+")

    __VALID_WHERE_OPERATION_LIST = [
        "=", "==", "!=", "<>", ">", ">=", "<", "<=",
    ]

    @classmethod
    def sanitize(cls, query_item):
        """
        Sanitize SQLite query with an empty char.

        :param str query_item: String to be sanitized.
        :return:
            String that exclude invalid chars.
            Invalid operators are as follows:
            ``"%"``, ``"/"``, ``"("``, ``")"``, ``"["``, ``"]"``,
            ``"<"``, ``">"``, ``"."``, ``":"``, ``";"``, ``"'"``,
            ``"!"``, ``"\"``, ``"#"``, ``"-"``, ``"+"``, ``"="``,
            ``"\\n"``, ``"\\r"``
        :rtype: str

        :Examples:

            >>> from simplesqlite.sqlquery import SqlQuery
            >>> SqlQuery.sanitize("k<e:y")
            'key'
        """

        return cls.__RE_SANITIZE.sub("", query_item)

    @classmethod
    def sanitize_attr(cls, query_item):
        return cls.__RE_SANITIZE_ATTR.sub("_", query_item)

    @classmethod
    def to_table_str(cls, name):
        """
        :param str name: Table name.
        :return: String that suitable for table name of a SQLite query.
        :rtype: str

        :Examples:

            >>> from simplesqlite.sqlquery import SqlQuery
            >>> SqlQuery.to_table_str("length")
            'length'
            >>> SqlQuery.to_table_str("length(cm)")
            '[length(cm)]'
            >>> SqlQuery.to_table_str("string length")
            "'string length'"
        """

        if cls.__RE_TABLE_STR.search(name):
            return "[{:s}]".format(name)

        if cls.__RE_SPACE.search(name):
            return "'{:s}'".format(name)

        return name

    @classmethod
    def to_attr_str(cls, name, operation_query=""):
        """
        :param str name: Attribute name.
        :param str operation_query:
            Used as a SQLite function if the value is not empty.
        :return: String that suitable for attribute name of a SQLite query.
        :rtype: str

        :Examples:

            >>> from simplesqlite.sqlquery import SqlQuery
            >>> SqlQuery.to_attr_str("key")
            'key'
            >>> SqlQuery.to_attr_str("a+b")
            '[a+b]'
            >>> SqlQuery.to_attr_str("key", operation_query="SUM")
            'SUM(key)'
        """

        name = cls.sanitize_attr(name)

        is_quote = cls.__RE_TO_ATTR_QUOTE.search(name) is not None

        try:
            pv.validate_sqlite_attr_name(name)
        except pv.InvalidReservedNameError:
            is_quote = True
        except (pv.ValidReservedNameError, pv.NullNameError, pv.InvalidCharError):
            pass

        if is_quote:
            sql_name = '"{:s}"'.format(name)
        elif cls.__RE_TO_ATTR_BRACKET.search(name):
            sql_name = "[{:s}]".format(name)
        elif name == "join":
            sql_name = "[{:s}]".format(name)
        else:
            sql_name = name

        if dp.is_not_empty_string(operation_query):
            sql_name = "{:s}({:s})".format(operation_query, sql_name)

        return sql_name

    @classmethod
    def to_attr_str_list(cls, name_list, operation_query=""):
        """
        :param list/tuple name_list: List of attribute names.
        :param str operation_query:
            Used as a SQLite function if the value is not empty.
        :return:
            List of strings that suitable for
            attribute names of a SQLite query.
        :rtype: list/itertools.imap

        :Examples:

            >>> from simplesqlite.sqlquery import SqlQuery
            >>> list(SqlQuery.to_attr_str_list(["key", "a+b"]))
            ['key', '[a+b]']
            >>> SqlQuery.to_attr_str_list(["key", "a+b"], operation_query="AVG")
            ['AVG(key)', 'AVG([a+b])']

        .. seealso::

            :py:meth:`.to_attr_str`
        """

        if dp.is_empty_string(operation_query):
            return list(map(cls.to_attr_str, name_list))

        return [
            "{:s}({:s})".format(operation_query, cls.to_attr_str(name))
            for name in name_list
        ]

    @classmethod
    def to_value_str(cls, value):
        """
        :param str value: Value associated with a key.
        :return:
            String that suitable for a value of a key.
            Return ``"NULL"`` if the value is |None|.
        :rtype: str

        :Examples:

            >>> from simplesqlite.sqlquery import SqlQuery
            >>> SqlQuery.to_value_str(1.2)
            '1.2'
            >>> SqlQuery.to_value_str("value")
            "'value'"
            >>> SqlQuery.to_value_str(None)
            'NULL'
        """

        from dataproperty import (IntegerType, FloatType)

        if value is None:
            return "NULL"

        if any([
            IntegerType(value).is_type(),
            FloatType(value).is_type()
        ]):
            return str(value)

        return "'{}'".format(value)

    @classmethod
    def to_value_str_list(cls, value_list):
        """
        :param list value_list: List of values associated with a key.
        :return:
            List of values that executed ``to_value_str`` method for each item.
        :rtype: itertools.imap

        :Examples:

            >>> from simplesqlite.sqlquery import SqlQuery
            >>> list(SqlQuery.to_value_str_list([1, "a", None]))
            ['1', "'a'", 'NULL']

        .. seealso::

            :py:meth:`.to_value_str`
        """

        return map(cls.to_value_str, value_list)

    @classmethod
    def make_select(cls, select, table, where=None, extra=None):
        """
        Make SELECT query.

        :param str select: Attribute for SELECT query.
        :param str table: Table name of executing the query.
        :param str where:
            Add a WHERE clause to execute query,
            if the value is not |None|.
        :param extra extra:
            Add additional clause to execute query,
            if the value is not |None|.
        :return: Query of SQLite.
        :rtype: str
        :raises ValueError: ``select`` is empty string.
        :raises simplesqlite.InvalidTableNameError:
            |raises_validate_table_name|

        :Examples:

            >>> from simplesqlite.sqlquery import SqlQuery
            >>> SqlQuery.make_select(select="value", table="example")
            'SELECT value FROM example'
            >>> SqlQuery.make_select(select="value", table="example", where=SqlQuery.make_where("key", 1))
            'SELECT value FROM example WHERE key = 1'
            >>> SqlQuery.make_select(select="value", table="example", where=SqlQuery.make_where("key", 1), extra="ORDER BY value")
            'SELECT value FROM example WHERE key = 1 ORDER BY value'
        """

        validate_table_name(table)
        if dp.is_empty_string(select):
            raise ValueError("SELECT query is null")

        query_list = [
            "SELECT " + select,
            "FROM " + cls.to_table_str(table),
        ]
        if dp.is_not_empty_string(where):
            query_list.append("WHERE " + where)
        if dp.is_not_empty_string(extra):
            query_list.append(extra)

        return " ".join(query_list)

    @classmethod
    def make_insert(cls, table, insert_tuple, is_insert_many=False):
        """
        Make INSERT query.

        :param str table: Table name of executing the query.
        :param list/tuple insert_tuple: Insertion data.
        :param bool is_insert_many:
            Make query that insert multiple data at once,
            if the value is |True|.
        :return: Query of SQLite.
        :rtype: str
        :raises ValueError: If ``insert_tuple`` is empty |list|/|tuple|.
        :raises simplesqlite.InvalidTableNameError:
            |raises_validate_table_name|
        """

        validate_table_name(table)

        table = cls.to_table_str(table)

        if dp.is_empty_sequence(insert_tuple):
            raise ValueError("empty insert list/tuple")

        return "INSERT INTO {:s} VALUES ({:s})".format(
            table, ",".join(['?' for _i in insert_tuple]))

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
        :raises simplesqlite.InvalidTableNameError:
            |raises_validate_table_name|
        """

        validate_table_name(table)
        if dp.is_empty_string(set_query):
            raise ValueError("SET query is null")

        query_list = [
            "UPDATE " + cls.to_table_str(table),
            "SET " + set_query,
        ]
        if dp.is_not_empty_string(where):
            query_list.append("WHERE " + where)

        return " ".join(query_list)

    @classmethod
    def make_where(cls, key, value, operation="="):
        """
        Make part of WHERE query.

        :param str key: Attribute name of the key.
        :param str value: Value of the right hand side associated with the key.
        :param str operation: Operator of WHERE query.
        :return: Part of WHERE query of SQLite.
        :rtype: str
        :raises simplesqlite.SqlSyntaxError:
            If **a)** ``operation`` is invalid operator.
            Valid operators are as follows:
            ``"="``, ``"=="``, ``"!="``, ``"<>"``,
            ``">"``, ``">="``, ``"<"``, ``"<="``.
            **b)** the ``value`` is |None| and
            the ``operation`` is not ``"="``/``"!="``.

        :Examples:

            >>> from simplesqlite.sqlquery import SqlQuery
            >>> SqlQuery.make_where("key", "hoge")
            "key = 'hoge'"
            >>> SqlQuery.make_where("value", 1, operation=">")
            'value > 1'
        """

        if operation not in cls.__VALID_WHERE_OPERATION_LIST:
            raise SqlSyntaxError("operation not supported: " + str(operation))

        if value is not None:
            return "{:s} {:s} {:s}".format(
                cls.to_attr_str(key), operation, cls.to_value_str(value))

        if operation == "=":
            return "{:s} IS NULL".format(cls.to_attr_str(key))
        elif operation == "!=":
            return "{:s} IS NOT NULL".format(cls.to_attr_str(key))

        raise SqlSyntaxError(
            "Invalid operation ({:s}) with None right-hand side".format(
                operation))

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
            cls.to_attr_str(key), ", ".join(cls.to_value_str_list(value_list)))

    @classmethod
    def make_where_not_in(cls, key, value_list):
        """
        Make part of WHERE NOT IN query.

        :param str key: Attribute name of the key.
        :param str value_list:
            List of values that the right hand side associated with the key.
        :return: Part of WHERE query of SQLite.
        :rtype: str

        :Examples:

            >>> from simplesqlite.sqlquery import SqlQuery
            >>> SqlQuery.make_where_not_in("key", ["hoge", "foo", "bar"])
            "key NOT IN ('hoge', 'foo', 'bar')"
        """

        return "{:s} NOT IN ({:s})".format(
            cls.to_attr_str(key), ", ".join(cls.to_value_str_list(value_list)))
