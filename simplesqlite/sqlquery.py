# encoding: utf-8

'''
@author: Tsuyoshi Hombashi
'''


from __future__ import absolute_import
import re

import dataproperty
import six
from six.moves import map

import simplesqlite as sql


class SqlQuery:
    """
    Support class for making SQLite query.
    """

    __RE_SANITIZE = re.compile("[%s]" % (re.escape("%/()[]<>.:;'!\# -+=\n\r")))
    __RE_TABLE_STR = re.compile("[%s]" % (re.escape("%()-+/.")))
    __RE_TO_ATTR_STR = re.compile("[%s0-9\s#]" % (re.escape("[%()-+/.]")))
    __RE_SPACE = re.compile("[\s]+")

    __VALID_WHERE_OPERATION_LIST = [
        "=", "==", "!=", "<>", ">", ">=", "<", "<=",
    ]

    @classmethod
    def sanitize(cls, query_item):
        """
        Sanitize SQLite query with empty char.

        :param str query_item: String to be sanitize.
        :return:
            String that exclude invalid chars.
            Invalid operators are as follows:
            ``"%"``, ``"/"``, ``"("``, ``")"``, ``"["``, ``"]"``,
            ``"<"``, ``">"``, ``"."``, ``":"``, ``";"``, ``"'"``,
            ``"!"``, ``"\"``, ``"#"``, ``"-"``, ``"+"``, ``"="``,
            ``"\\n"``, ``"\\r"``
        :rtype: str
        """

        return cls.__RE_SANITIZE.sub("", query_item)

    @classmethod
    def to_table_str(cls, name):
        """
        :param str name: Base name of table.
        :return: String that suitable for table name.
        :rtype: str
        """

        if cls.__RE_TABLE_STR.search(name):
            return "[%s]" % (name)

        if cls.__RE_SPACE.search(name):
            return "'%s'" % (name)

        return name

    @classmethod
    def to_attr_str(cls, name, operation_query=""):
        """
        :param str name: Base name of attribute.
        :param str operation_query:
            Used as a SQLite function if the value is not empty.
        :return: String that suitable for attribute name.
        :rtype: str
        """

        if cls.__RE_TO_ATTR_STR.search(name):
            sql_name = "[%s]" % (name)
        elif name == "join":
            sql_name = "[%s]" % (name)
        else:
            sql_name = name

        if dataproperty.is_not_empty_string(operation_query):
            sql_name = "%s(%s)" % (operation_query, sql_name)

        return sql_name

    @classmethod
    def to_attr_str_list(cls, name_list, operation_query=""):
        """
        :param list/tuple name_list: List of attribute names.
        :param str operation_query:
            Used as a SQLite function if the value is not empty.
        :return: List of strings that suitable for attribute name.
        :rtype: list

        .. seealso::

            :py:meth:`.to_attr_str`
        """

        if dataproperty.is_empty_string(operation_query):
            return map(cls.to_attr_str, name_list)

        return [
            "%s(%s)" % (operation_query, cls.to_attr_str(name))
            for name in name_list
        ]

    @classmethod
    def to_value_str(cls, value):
        """
        :param str value: Value associated with a key.
        :return:
            String that suitable for value of a key.
            Return ``"NULL"`` if the value is ``None``
        :rtype: str
        """

        if value is None:
            return "NULL"

        if dataproperty.is_integer(value) or dataproperty.is_float(value):
            return str(value)

        return "'%s'" % (value)

    @classmethod
    def to_value_str_list(cls, value_list):
        """
        :param list value_list: List of values associated with a key.
        :return:
            List of value that executed ``to_value_str`` method for each item.
        :rtype: list

        .. seealso::

            :py:meth:`.to_value_str`
        """

        return map(cls.to_value_str, value_list)

    @classmethod
    def make_select(cls, select, table, where=None, extra=None):
        """
        Make SELECT query.

        :param str select: Attribute for SELECT query.
        :param str table: Table name of execute query.
        :param str where:
            Add WHERE clause to execute query,
            if the value is not ``None``.
        :param extra extra:
            Add additional clause to execute query,
            if the value is not ``None``.
        :return: Query of SQLite.
        :rtype: str

        :raises ValueError: ``select`` is empty string.

        .. seealso::

            :py:func:`simplesqlite.validate_table_name`
        """

        sql.validate_table_name(table)
        if dataproperty.is_empty_string(select):
            raise ValueError("SELECT query is null")

        query_list = [
            "SELECT " + select,
            "FROM " + cls.to_table_str(table),
        ]
        if dataproperty.is_not_empty_string(where):
            query_list.append("WHERE " + where)
        if dataproperty.is_not_empty_string(extra):
            query_list.append(extra)

        return " ".join(query_list)

    @classmethod
    def make_insert(cls, table, insert_tuple, is_insert_many=False):
        """
        Make INSERT query.

        :param str table: Table name of execute query.
        :param list/tuple insert_tuple: Insertion data.
        :param bool is_insert_many:
            Make query that insert multiple data at once,
            if the value is ``True``.
        :return: Query of SQLite.
        :rtype: str
        :raises ValueError: If ``insert_tuple`` is empty list/tuple.

        .. seealso::

            :py:func:`simplesqlite.validate_table_name`
        """

        sql.validate_table_name(table)

        table = cls.to_table_str(table)

        if dataproperty.is_empty_list_or_tuple(insert_tuple):
            raise ValueError("empty insert list/tuple")

        if is_insert_many:
            value_list = ['?' for _i in insert_tuple]
        else:
            value_list = [
                "'%s'" % (value)
                if isinstance(value, six.string_types) and value != "NULL"
                else str(value)
                for value in insert_tuple
            ]

        return "INSERT INTO %s VALUES (%s)" % (
            table, ",".join(value_list))

    @classmethod
    def make_update(cls, table, set_query, where=None):
        """
        Make UPDATE query.

        :param str table: Table name of execute query.
        :param str set_query: SET part of UPDATE query.
        :param str where:
            Add WHERE clause to execute query,
            if the value is not ``None``.
        :return: Query of SQLite.
        :rtype: str
        :raises ValueError: If ``set_query`` is empty string.

        .. seealso::

            :py:func:`simplesqlite.validate_table_name`
        """

        sql.validate_table_name(table)
        if dataproperty.is_empty_string(set_query):
            raise ValueError("SET query is null")

        query_list = [
            "UPDATE " + cls.to_table_str(table),
            "SET " + set_query,
        ]
        if dataproperty.is_not_empty_string(where):
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

        :raises ValueError:
            If ``operation`` is invalid operator.
            Valid operators are as follows:
            ``"="``, ``"=="``, ``"!="``, ``"<>"``,
            ``">"``, ``">="``, ``"<"``, ``"<="``
        """

        if operation not in cls.__VALID_WHERE_OPERATION_LIST:
            raise ValueError("operation not supported: " + str(operation))

        return "%s %s %s" % (
            cls.to_attr_str(key), operation, cls.to_value_str(value))

    @classmethod
    def make_where_in(cls, key, value_list):
        """
        Make part of WHERE IN query.

        :param str key: Attribute name of the key.
        :param str value_list:
            List of values that the right hand side associated with the key.
        :return: Part of WHERE query of SQLite.
        :rtype: str
        """

        return "%s IN (%s)" % (
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
        """

        return "%s NOT IN (%s)" % (
            cls.to_attr_str(key), ", ".join(cls.to_value_str_list(value_list)))
