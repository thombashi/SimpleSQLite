# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

import logging
import os
import re
import sqlite3

import dataproperty
import msgfy
import pathvalidate
import six
import typepy
from mbstrdecoder import MultiByteStrDecoder
from sqliteschema import SQLITE_SYSTEM_TABLE_LIST, SQLiteSchemaExtractor
from tabledata import TableData

from ._func import validate_table_name
from ._logger import logger
from ._sanitizer import SQLiteTableDataSanitizer
from .converter import RecordConvertor
from .error import (
    AttributeNotFoundError,
    DatabaseError,
    NameValidationError,
    NullDatabaseConnectionError,
    OperationalError,
    TableNotFoundError,
)
from .query import Attr, AttrList, Select, Table, Value, make_index_name
from .sqlquery import SqlQuery


MEMORY_DB_NAME = ":memory:"


class SimpleSQLite(object):
    """
    Wrapper class for |sqlite3| module.

    :param str database_src:
        SQLite database source:
        (1) File path to a database to be connected.
        (2) sqlite3.Connection instance.
    :param str mode: Open mode.
    :param bool profile:
        Recording SQL query execution time profile, if the value is |True|.

    .. seealso::
        :py:meth:`.connect`
        :py:meth:`.get_profile`
    """

    dup_col_handler = "error"

    @property
    def database_path(self):
        """
        :return: File path of the connected database.
        :rtype: str

        :Examples:
            >>> from simplesqlite import SimpleSQLite
            >>> con = SimpleSQLite("sample.sqlite", "w")
            >>> con.database_path
            '/tmp/sample.sqlite'
            >>> con.close()
            >>> print(con.database_path)
            None
        """

        if self.__delayed_connection_path:
            return self.__delayed_connection_path

        return self.__database_path

    @property
    def connection(self):
        """
        :return: |Connection| instance of the connected database.
        :rtype: sqlite3.Connection
        """

        self.__delayed_connect()

        return self.__connection

    @property
    def total_changes(self):
        """
        .. seealso::
            :py:attr:`sqlite3.Connection.total_changes`
        """

        self.check_connection()

        return self.connection.total_changes

    @property
    def mode(self):
        """
        :return: Connection mode: ``"r"``/``"w"``/``"a"``.
        :rtype: str

        .. seealso:: :py:meth:`.connect`
        """

        return self.__mode

    def __init__(self, database_src, mode="a", delayed_connection=True, profile=False):
        self.debug_query = False

        self.__initialize_connection()
        self.__mode = mode
        self.__is_profile = profile

        if isinstance(database_src, sqlite3.Connection):
            self.__connection = database_src
            return

        if delayed_connection:
            self.__delayed_connection_path = database_src
            return

        self.connect(database_src, mode)

    def __del__(self):
        self.close()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def is_connected(self):
        """
        :return: |True| if the connection to a database is valid.
        :rtype: bool

        :Examples:
            >>> from simplesqlite import SimpleSQLite
            >>> con = SimpleSQLite("sample.sqlite", "w")
            >>> con.is_connected()
            True
            >>> con.close()
            >>> con.is_connected()
            False
        """

        try:
            self.check_connection()
        except NullDatabaseConnectionError:
            return False

        return True

    def check_connection(self):
        """
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|

        :Sample Code:
            .. code:: python

                import simplesqlite

                con = simplesqlite.SimpleSQLite("sample.sqlite", "w")

                print("---- connected to a database ----")
                con.check_connection()

                print("---- disconnected from a database ----")
                con.close()
                try:
                    con.check_connection()
                except simplesqlite.NullDatabaseConnectionError as e:
                    print(e)

        :Output:
            .. code-block:: none

                ---- connected to a database ----
                ---- disconnected from a database ----
                null database connection
        """

        if self.connection is None:
            if self.__delayed_connect():
                return

            raise NullDatabaseConnectionError("null database connection")

        if typepy.is_null_string(self.database_path):
            raise NullDatabaseConnectionError("null database file path")

    def connect(self, database_path, mode="a"):
        """
        Connect to a SQLite database.

        :param str database_path:
            Path to the SQLite database file to be connected.
        :param str mode:
            ``"r"``: Open for read only.
            ``"w"``: Open for read/write.
            Delete existing tables when connecting.
            ``"a"``: Open for read/write. Append to the existing tables.
        :raises ValueError:
            If ``database_path`` is invalid or |attr_mode| is invalid.
        :raises simplesqlite.DatabaseError:
            If the file is encrypted or is not a database.
        :raises simplesqlite.OperationalError:
            If unable to open the database file.
        """

        self.close()

        logger.debug("connect to a SQLite database: path='{}', mode={}".format(database_path, mode))

        if mode == "r":
            self.__verify_db_file_existence(database_path)
        elif mode in ["w", "a"]:
            self.__validate_db_path(database_path)
        else:
            raise ValueError("unknown connection mode: " + mode)

        if database_path == MEMORY_DB_NAME:
            self.__database_path = database_path
        else:
            self.__database_path = os.path.realpath(database_path)

        try:
            self.__connection = sqlite3.connect(database_path)
        except sqlite3.OperationalError as e:
            raise OperationalError(e)

        self.__mode = mode

        try:
            # validate connection after connect
            self.fetch_table_name_list()
        except sqlite3.DatabaseError as e:
            raise DatabaseError(e)

        if mode != "w":
            return

        for table in self.fetch_table_name_list():
            self.drop_table(table)

    def execute_query(self, query, caller=None):
        """
        Send arbitrary SQLite query to the database.

        :param str query: Query to executed.
        :param tuple caller:
            Caller information.
            Expects the return value of :py:meth:`logging.Logger.findCaller`.
        :return: The result of the query execution.
        :rtype: sqlite3.Cursor
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        .. warning::
            This method can execute an arbitrary query.
            i.e. No access permissions check by |attr_mode|.
        """

        import time

        self.check_connection()
        if typepy.is_null_string(query):
            return None

        if self.debug_query:
            logger.debug(query)

        if self.__is_profile:
            exec_start_time = time.time()

        try:
            result = self.connection.execute(six.text_type(query))
        except sqlite3.OperationalError as e:
            if caller is None:
                caller = logging.getLogger().findCaller()
            file_path, line_no, func_name = caller[:3]
            message_list = [
                "failed to execute query at {:s}({:d}) {:s}".format(file_path, line_no, func_name),
                "  - query: {}".format(MultiByteStrDecoder(query).unicode_str),
                "  - msg:   {}".format(str(e)),
                "  - db:    {}".format(self.database_path),
            ]
            raise OperationalError(message="\n".join(message_list))

        if self.__is_profile:
            self.__dict_query_count[query] = self.__dict_query_count.get(query, 0) + 1

            elapse_time = time.time() - exec_start_time
            self.__dict_query_totalexectime[query] = (
                self.__dict_query_totalexectime.get(query, 0) + elapse_time
            )

        return result

    def set_row_factory(self, row_factory):
        """
        Set row_factory to the database connection.
        """

        self.check_connection()

        self.__connection.row_factory = row_factory

    def select(self, select, table_name, where=None, extra=None):
        """
        Send a SELECT query to the database.

        :param str select: Attribute for the ``SELECT`` query.
        :param str table_name: |arg_select_table_name|
        :param str where: |arg_select_where|
        :param str extra: |arg_select_extra|
        :return: Result of the query execution.
        :rtype: sqlite3.Cursor
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|
        """

        self.verify_table_existence(table_name)

        return self.execute_query(
            six.text_type(Select(select, table_name, where, extra)),
            logging.getLogger().findCaller(),
        )

    def select_as_dataframe(self, table_name, column_list=None, where=None, extra=None):
        """
        Get data in the database and return fetched data as a
        :py:class:`pandas.Dataframe` instance.

        :param str table_name: |arg_select_table_name|
        :param list column_list: |arg_select_as_xx_column_list|
        :param str where: |arg_select_where|
        :param str extra: |arg_select_extra|
        :return: Table data as a :py:class:`pandas.Dataframe` instance.
        :rtype: pandas.DataFrame
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Example:
            :ref:`example-select-as-dataframe`

        .. note::
            ``pandas`` package required to execute this method.
        """

        import pandas

        if column_list is None:
            column_list = self.fetch_attr_name_list(table_name)

        result = self.select(
            select=AttrList(column_list), table_name=table_name, where=where, extra=extra
        )

        if result is None:
            return pandas.DataFrame()

        return pandas.DataFrame(result.fetchall(), columns=column_list)

    def select_as_tabledata(self, table_name, column_list=None, where=None, extra=None):
        """
        Get data in the database and return fetched data as a
        :py:class:`tabledata.TableData` instance.

        :param str table_name: |arg_select_table_name|
        :param list column_list: |arg_select_as_xx_column_list|
        :param str where: |arg_select_where|
        :param str extra: |arg_select_extra|
        :return: Table data as a :py:class:`tabledata.TableData` instance.
        :rtype: tabledata.TableData
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        .. note::
            ``pandas`` package required to execute this method.
        """

        if column_list is None:
            column_list = self.fetch_attr_name_list(table_name)

        result = self.select(
            select=AttrList(column_list), table_name=table_name, where=where, extra=extra
        )

        if result is None:
            return TableData(table_name=None, header_list=[], row_list=[])

        return TableData(table_name=table_name, header_list=column_list, row_list=result.fetchall())

    def select_as_dict(self, table_name, column_list=None, where=None, extra=None):
        """
        Get data in the database and return fetched data as a
        |OrderedDict| list.

        :param str table_name: |arg_select_table_name|
        :param list column_list: |arg_select_as_xx_column_list|
        :param str where: |arg_select_where|
        :param str extra: |arg_select_extra|
        :return: Table data as |OrderedDict| instances.
        :rtype: |list| of |OrderedDict|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Example:
            :ref:`example-select-as-dict`
        """

        return (
            self.select_as_tabledata(table_name, column_list, where, extra)
            .as_dict()
            .get(table_name)
        )

    def select_as_memdb(self, table_name, column_list=None, where=None, extra=None):
        """
        Get data in the database and return fetched data as a
        in-memory |SimpleSQLite| instance.

        :param str table_name: |arg_select_table_name|
        :param list column_list: |arg_select_as_xx_column_list|
        :param str where: |arg_select_where|
        :param str extra: |arg_select_extra|
        :return:
            Table data as a |SimpleSQLite| instance that connected to in
            memory database.
        :rtype: |SimpleSQLite|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|
        """

        memdb = connect_memdb()
        memdb.create_table_from_tabledata(
            self.select_as_tabledata(table_name, column_list, where, extra)
        )

        return memdb

    def insert(self, table_name, record):
        """
        Send an INSERT query to the database.

        :param str table_name: Table name of executing the query.
        :param record: Record to be inserted.
        :type record: |dict|/|namedtuple|/|list|/|tuple|
        :raises IOError: |raises_write_permission|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Example:
            :ref:`example-insert-records`
        """

        self.insert_many(table_name, [record])

    def insert_many(self, table_name, record_list):
        """
        Send an INSERT query with multiple records to the database.

        :param str table: Table name of executing the query.
        :param record_list: Records to be inserted.
        :type record_list: |dict|/|namedtuple|/|list|/|tuple|
        :return: Number of inserted records.
        :rtype: int
        :raises IOError: |raises_write_permission|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Example:
            :ref:`example-insert-records`
        """

        self.validate_access_permission(["w", "a"])
        self.verify_table_existence(table_name)

        logger.debug(
            "insert {number} records into {table}".format(
                number=len(record_list) if record_list else 0, table=table_name
            )
        )

        if typepy.is_empty_sequence(record_list):
            return 0

        record_list = RecordConvertor.to_record_list(
            self.fetch_attr_name_list(table_name), record_list
        )
        query = SqlQuery.make_insert(table_name, record_list[0])

        if self.debug_query:
            logger.debug(query)
            for i, record in enumerate(record_list):
                logger.debug("    record {:4d}: {}".format(i, record))

        try:
            self.connection.executemany(query, record_list)
        except sqlite3.OperationalError as e:
            caller = logging.getLogger().findCaller()
            file_path, line_no, func_name = caller[:3]
            raise OperationalError(
                "{:s}({:d}) {:s}: failed to execute query:\n".format(file_path, line_no, func_name)
                + "  query={}\n".format(query)
                + "  msg='{}'\n".format(str(e))
                + "  db={}\n".format(self.database_path)
                + "  records={}\n".format(record_list[:2])
            )

        return len(record_list)

    def update(self, table_name, set_query, where=None):
        """
        Send an UPDATE query to the database.

        :param str table_name: Table name of executing the query.
        :param str set_query:
        :param str where:
        :raises IOError: |raises_write_permission|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|
        """

        self.validate_access_permission(["w", "a"])
        self.verify_table_existence(table_name)

        query = SqlQuery.make_update(table_name, set_query, where)

        return self.execute_query(query, logging.getLogger().findCaller())

    def delete(self, table_name, where=None):
        """
        Send a DELETE query to the database.

        :param str table_name: Table name of executing the query.
        :param str where:
        """

        self.validate_access_permission(["w", "a"])
        self.verify_table_existence(table_name)

        query = "DELETE FROM {:s}".format(table_name)
        if where:
            query += " WHERE {:s}".format(where)

        return self.execute_query(query, logging.getLogger().findCaller())

    def fetch_value(self, select, table_name, where=None, extra=None):
        """
        Fetch a value from the table. Return |None| if no value matches
        the conditions, or the table not found in the database.

        :param str select: Attribute for SELECT query
        :param str table_name: Table name of executing the query.
        :return: Result of execution of the query.
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.OperationalError: |raises_operational_error|
        """

        try:
            self.verify_table_existence(table_name)
        except TableNotFoundError as e:
            logger.debug(msgfy.to_debug_message(e))
            return None

        result = self.execute_query(
            Select(select, table_name, where, extra), logging.getLogger().findCaller()
        )
        if result is None:
            return None

        fetch = result.fetchone()
        if fetch is None:
            return None

        return fetch[0]

    def fetch_table_name_list(self, include_system_table=False):
        """
        :return: List of table names in the database.
        :rtype: list
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Sample Code:
            .. code:: python

                from simplesqlite import SimpleSQLite

                con = SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name="hoge",
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])
                print(con.fetch_table_name_list())
        :Output:
            .. code-block:: python

                ['hoge']
        """

        self.check_connection()

        return SQLiteSchemaExtractor(self).fetch_table_name_list()

    def fetch_attr_name_list(self, table_name):
        """
        :return: List of attribute names in the table.
        :rtype: list
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Example:
            .. code:: python

                import simplesqlite

                table_name = "sample_table"
                con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name,
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])

                print(con.fetch_attr_name_list(table_name))

                try:
                    print(con.fetch_attr_name_list("not_existing"))
                except simplesqlite.TableNotFoundError as e:
                    print(e)
        :Output:
            .. parsed-literal::

                ['attr_a', 'attr_b']
                'not_existing' table not found in /tmp/sample.sqlite
        """

        self.verify_table_existence(table_name)

        return SQLiteSchemaExtractor(self).fetch_table_schema(table_name).get_attr_name_list()

    def fetch_attr_type(self, table_name):
        """
        :return:
            Dictionary of attribute names and attribute types in the table.
        :rtype: dict
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|
        """

        self.verify_table_existence(table_name)

        result = self.execute_query(
            "SELECT sql FROM sqlite_master WHERE type='table' and name={:s}".format(
                Value(table_name)
            )
        )
        query = result.fetchone()[0]
        match = re.search("[(].*[)]", query)

        def get_entry(item_list):
            key = " ".join(item_list[:-1])
            value = item_list[-1]

            return [key, value]

        return dict([get_entry(item.split(" ")) for item in match.group().strip("()").split(", ")])

    def fetch_num_records(self, table_name, where=None):
        """
        Fetch the number of records in a table.

        :param str table_name: Table name to get number of records.
        :param str where: Where clause for the query.
        :return:
            Number of records in the table.
            |None| if no value matches the conditions,
            or the table not found in the database.
        :rtype: int
        """

        return self.fetch_value(select="COUNT(*)", table_name=table_name, where=where)

    def get_profile(self, profile_count=50):
        """
        Get profile of query execution time.

        :param int profile_count:
            Number of profiles to retrieve,
            counted from the top query in descending order by
            the cumulative execution time.
        :return: Profile information for each query.
        :rtype: list of |namedtuple|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Example:
            :ref:`example-get-profile`
        """

        from collections import namedtuple

        profile_table_name = "sql_profile"

        value_matrix = [
            [query, execute_time, self.__dict_query_count.get(query, 0)]
            for query, execute_time in six.iteritems(self.__dict_query_totalexectime)
        ]
        attr_name_list = ("sql_query", "cumulative_time", "count")
        con_tmp = connect_memdb()
        try:
            con_tmp.create_table_from_data_matrix(
                profile_table_name, attr_name_list, data_matrix=value_matrix
            )
        except ValueError:
            return []

        try:
            result = con_tmp.select(
                select="{:s},SUM({:s}),SUM({:s})".format(*attr_name_list),
                table_name=profile_table_name,
                extra="GROUP BY {:s} ORDER BY {:s} DESC LIMIT {:d}".format(
                    attr_name_list[0], attr_name_list[1], profile_count
                ),
            )
        except sqlite3.OperationalError:
            return []
        if result is None:
            return []

        SqliteProfile = namedtuple("SqliteProfile", " ".join(attr_name_list))

        return [SqliteProfile(*profile) for profile in result.fetchall()]

    def fetch_sqlite_master(self):
        """
        Get sqlite_master table information as a list of dictionaries.

        :return: sqlite_master table information.
        :rtype: list
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|

        :Sample Code:
            .. code:: python

                import json

                from simplesqlite import SimpleSQLite

                con = SimpleSQLite("sample.sqlite", "w")
                data_matrix = [
                    [1, 1.1, "aaa", 1,   1],
                    [2, 2.2, "bbb", 2.2, 2.2],
                    [3, 3.3, "ccc", 3,   "ccc"],
                ]
                con.create_table_from_data_matrix(
                    table_name="sample_table",
                    attr_name_list=["a", "b", "c", "d", "e"],
                    data_matrix=data_matrix,
                    index_attr_list=["a"])

                print(json.dumps(con.fetch_sqlite_master(), indent=4))
        :Output:
            .. code-block:: json

                [
                    {
                        "tbl_name": "sample_table",
                        "sql": "CREATE TABLE 'sample_table' ('a' INTEGER, 'b' REAL, 'c' TEXT, 'd' REAL, 'e' TEXT)",
                        "type": "table",
                        "name": "sample_table",
                        "rootpage": 2
                    },
                    {
                        "tbl_name": "sample_table",
                        "sql": "CREATE INDEX sample_table_a_index ON sample_table('a')",
                        "type": "index",
                        "name": "sample_table_a_index",
                        "rootpage": 3
                    }
                ]
        """

        self.check_connection()

        return SQLiteSchemaExtractor(self).fetch_sqlite_master()

    def has_table(self, table_name):
        """
        :param str table_name: Table name to be tested.
        :return: |True| if the database has the table.
        :rtype: bool

        :Sample Code:
            .. code:: python

                from simplesqlite import SimpleSQLite

                con = SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name="hoge",
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])

                print(con.has_table("hoge"))
                print(con.has_table("not_existing"))
        :Output:
            .. code-block:: python

                True
                False
        """

        try:
            validate_table_name(table_name)
        except NameValidationError:
            return False

        return table_name in self.fetch_table_name_list()

    def has_attr(self, table_name, attr_name):
        """
        :param str table_name: Table name that the attribute exists.
        :param str attr_name: Attribute name to be tested.
        :return: |True| if the table has the attribute.
        :rtype: bool
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|

        :Sample Code:
            .. code:: python

                import simplesqlite

                table_name = "sample_table"
                con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name=table_name,
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])

                print(con.has_attr(table_name, "attr_a"))
                print(con.has_attr(table_name, "not_existing"))
                try:
                    print(con.has_attr("not_existing", "attr_a"))
                except simplesqlite.TableNotFoundError as e:
                    print(e)
        :Output:
            .. parsed-literal::

                True
                False
                'not_existing' table not found in /tmp/sample.sqlite
        """

        self.verify_table_existence(table_name)

        if typepy.is_null_string(attr_name):
            return False

        return attr_name in self.fetch_attr_name_list(table_name)

    def has_attr_list(self, table_name, attr_name_list):
        """
        :param str table_name: Table name that attributes exists.
        :param str attr_name_list: Attribute names to tested.
        :return: |True| if the table has all of the attribute.
        :rtype: bool
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|

        :Sample Code:
            .. code:: python

                import simplesqlite

                table_name = "sample_table"
                con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name=table_name,
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])

                print(con.has_attr_list(table_name, ["attr_a"]))
                print(con.has_attr_list(table_name, ["attr_a", "attr_b"]))
                print(con.has_attr_list(
                    table_name, ["attr_a", "attr_b", "not_existing"]))
                try:
                    print(con.has_attr("not_existing", ["attr_a"]))
                except simplesqlite.TableNotFoundError as e:
                    print(e)
        :Output:
            .. parsed-literal::

                True
                True
                False
                'not_existing' table not found in /tmp/sample.sqlite
        """

        if typepy.is_empty_sequence(attr_name_list):
            return False

        not_exist_field_list = [
            attr_name for attr_name in attr_name_list if not self.has_attr(table_name, attr_name)
        ]

        if not_exist_field_list:
            return False

        return True

    def verify_table_existence(self, table_name):
        """
        :param str table_name: Table name to be tested.
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.NameValidationError:
            |raises_validate_table_name|

        :Sample Code:
            .. code:: python

                import simplesqlite

                table_name = "sample_table"
                con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name,
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])

                con.verify_table_existence(table_name)
                try:
                    con.verify_table_existence("not_existing")
                except simplesqlite.TableNotFoundError as e:
                    print(e)
        :Output:
            .. parsed-literal::

                'not_existing' table not found in /tmp/sample.sqlite
        """

        validate_table_name(table_name)

        if self.has_table(table_name):
            return

        raise TableNotFoundError(
            "'{}' table not found in '{}' database".format(table_name, self.database_path)
        )

    def verify_attr_existence(self, table_name, attr_name):
        """
        :param str table_name: Table name that the attribute exists.
        :param str attr_name: Attribute name to tested.
        :raises simplesqlite.AttributeNotFoundError:
            If attribute not found in the table
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|

        :Sample Code:
            .. code:: python

                from simplesqlite import (
                    SimpleSQLite,
                    TableNotFoundError,
                    AttributeNotFoundError
                )

                table_name = "sample_table"
                con = SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name=table_name,
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])

                con.verify_attr_existence(table_name, "attr_a")
                try:
                    con.verify_attr_existence(table_name, "not_existing")
                except AttributeNotFoundError as e:
                    print(e)
                try:
                    con.verify_attr_existence("not_existing", "attr_a")
                except TableNotFoundError as e:
                    print(e)
        :Output:
            .. parsed-literal::

                'not_existing' attribute not found in 'sample_table' table
                'not_existing' table not found in /tmp/sample.sqlite
        """

        self.verify_table_existence(table_name)

        if self.has_attr(table_name, attr_name):
            return

        raise AttributeNotFoundError(
            "'{}' attribute not found in '{}' table".format(attr_name, table_name)
        )

    def validate_access_permission(self, valid_permission_list):
        """
        :param valid_permission_list:
            List of permissions that access is allowed.
        :type valid_permission_list: |list|/|tuple|
        :raises ValueError: If the |attr_mode| is invalid.
        :raises IOError:
            If the |attr_mode| not in the ``valid_permission_list``.
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        """

        self.check_connection()

        if typepy.is_null_string(self.mode):
            raise ValueError("mode is not set")

        if self.mode not in valid_permission_list:
            raise IOError(
                "invalid access: expected-mode='{}', current-mode='{}'".format(
                    "' or '".join(valid_permission_list), self.mode
                )
            )

    def drop_table(self, table_name):
        """
        :param str table_name: Table name to drop.
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises IOError: |raises_write_permission|
        """

        self.validate_access_permission(["w", "a"])

        if table_name in SQLITE_SYSTEM_TABLE_LIST:
            # warning message
            return

        if self.has_table(table_name):
            query = "DROP TABLE IF EXISTS '{:s}'".format(table_name)
            self.execute_query(query, logging.getLogger().findCaller())
            self.commit()

    def create_table(self, table_name, attr_description_list):
        """
        :param str table_name: Table name to create.
        :param list attr_description_list: List of table description.
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises IOError: |raises_write_permission|
        """

        self.validate_access_permission(["w", "a"])

        table_name = table_name.strip()
        if self.has_table(table_name):
            return True

        query = "CREATE TABLE IF NOT EXISTS '{:s}' ({:s})".format(
            table_name, ", ".join(attr_description_list)
        )
        logger.debug(query)

        if self.execute_query(query, logging.getLogger().findCaller()) is None:
            return False

        return True

    def create_index(self, table_name, attr_name):
        """
        :param str table_name:
            Table name that contains the attribute to be indexed.
        :param str attr_name: Attribute name to create index.
        :raises IOError: |raises_write_permission|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        """

        self.verify_table_existence(table_name)
        self.validate_access_permission(["w", "a"])

        query_format = "CREATE INDEX IF NOT EXISTS {index:s} ON {table}({attr})"
        query = query_format.format(
            index=make_index_name(table_name, attr_name),
            table=Table(table_name),
            attr=Attr(attr_name),
        )

        logger.debug(query)
        self.execute_query(query, logging.getLogger().findCaller())

    def create_index_list(self, table_name, attr_name_list):
        """
        :param str table_name: Table name that exists attribute.
        :param list attr_name_list:
            List of attribute names to create indices.
            Ignore attributes that are not existing in the table.

        .. seealso:: :py:meth:`.create_index`
        """

        self.validate_access_permission(["w", "a"])

        if typepy.is_empty_sequence(attr_name_list):
            return

        table_attr_set = set(self.fetch_attr_name_list(table_name))
        index_attr_set = set(AttrList.sanitize(attr_name_list))

        for attribute in list(table_attr_set.intersection(index_attr_set)):
            self.create_index(table_name, attribute)

    def create_table_from_data_matrix(
        self, table_name, attr_name_list, data_matrix, index_attr_list=None
    ):
        """
        Create a table if not exists. Moreover, insert data into the created
        table.

        :param str table_name: Table name to create.
        :param list attr_name_list: List of attribute names of the table.
        :param data_matrix: Data to be inserted into the table.
        :type data_matrix: List of |dict|/|namedtuple|/|list|/|tuple|
        :param tuple index_attr_list: |index_attr_list|
        :raises simplesqlite.NameValidationError:
            |raises_validate_table_name|
        :raises simplesqlite.NameValidationError:
            |raises_validate_attr_name|
        :raises ValueError: If the ``data_matrix`` is empty.

        :Example:
            :ref:`example-create-table-from-data-matrix`

        .. seealso::
            :py:meth:`.create_table`
            :py:meth:`.insert_many`
            :py:meth:`.create_index_list`
        """

        self.__create_table_from_tabledata(
            table_data=TableData(table_name, attr_name_list, data_matrix),
            index_attr_list=index_attr_list,
        )

    def create_table_from_tabledata(self, table_data, index_attr_list=None):
        """
        Create a table from :py:class:`tabledata.TableData`.

        :param tabledata.TableData table_data: Table data to create.
        :param tuple index_attr_list: |index_attr_list|

        .. seealso::
            :py:meth:`.create_table_from_data_matrix`
        """

        self.__create_table_from_tabledata(table_data=table_data, index_attr_list=index_attr_list)

    def create_table_from_csv(
        self,
        csv_source,
        table_name="",
        attr_name_list=(),
        delimiter=",",
        quotechar='"',
        encoding="utf-8",
        index_attr_list=None,
    ):
        """
        Create a table from a CSV file/text.

        :param str csv_source: Path to the CSV file or CSV text.
        :param str table_name:
            Table name to create.
            Using CSV file basename as the table name if the value is empty.
        :param list attr_name_list:
            Attribute names of the table.
            Use the first line of the CSV file as attribute list
            if attr_name_list is empty.
        :param str delimiter:
            A one-character string used to separate fields.
        :param str quotechar:
            A one-character string used to quote fields containing special
            characters, such as the ``delimiter`` or ``quotechar``,
            or which contain new-line characters.
        :param str encoding: CSV file encoding.
        :param tuple index_attr_list: |index_attr_list|
        :raises ValueError: If the CSV data is invalid.

        :Dependency Packages:
            - `pytablereader <https://github.com/thombashi/pytablereader>`__

        :Example:
            :ref:`example-create-table-from-csv`

        .. seealso::
            :py:meth:`.create_table_from_data_matrix`
            :py:func:`csv.reader`
            :py:meth:`.pytablereader.CsvTableFileLoader.load`
            :py:meth:`.pytablereader.CsvTableTextLoader.load`
        """

        import pytablereader as ptr

        loader = ptr.CsvTableFileLoader(csv_source)
        if typepy.is_not_null_string(table_name):
            loader.table_name = table_name
        loader.header_list = attr_name_list
        loader.delimiter = delimiter
        loader.quotechar = quotechar
        loader.encoding = encoding
        try:
            for table_data in loader.load():
                self.create_table_from_tabledata(table_data, index_attr_list=index_attr_list)
            return
        except (ptr.InvalidFilePathError, IOError):
            pass

        loader = ptr.CsvTableTextLoader(csv_source)
        if typepy.is_not_null_string(table_name):
            loader.table_name = table_name
        loader.header_list = attr_name_list
        loader.delimiter = delimiter
        loader.quotechar = quotechar
        loader.encoding = encoding
        for table_data in loader.load():
            self.create_table_from_tabledata(table_data, index_attr_list=index_attr_list)

    def create_table_from_json(self, json_source, table_name="", index_attr_list=None):
        """
        Create a table from a JSON file/text.

        :param str json_source: Path to the JSON file or JSON text.
        :param str table_name: Table name to create.
        :param tuple index_attr_list: |index_attr_list|

        :Dependency Packages:
            - `pytablereader <https://github.com/thombashi/pytablereader>`__

        :Examples:
            :ref:`example-create-table-from-json`

        .. seealso::
            :py:meth:`.pytablereader.JsonTableFileLoader.load`
            :py:meth:`.pytablereader.JsonTableTextLoader.load`
        """

        import pytablereader as ptr

        loader = ptr.JsonTableFileLoader(json_source)
        if typepy.is_not_null_string(table_name):
            loader.table_name = table_name
        try:
            for table_data in loader.load():
                self.create_table_from_tabledata(table_data, index_attr_list=index_attr_list)
            return
        except (ptr.InvalidFilePathError, IOError):
            pass

        loader = ptr.JsonTableTextLoader(json_source)
        if typepy.is_not_null_string(table_name):
            loader.table_name = table_name
        for table_data in loader.load():
            self.create_table_from_tabledata(table_data, index_attr_list=index_attr_list)

    def create_table_from_dataframe(self, dataframe, table_name="", index_attr_list=None):
        """
        Create a table from a pandas.DataFrame instance.

        :param pandas.DataFrame dataframe: DataFrame instance to convert.
        :param str table_name: Table name to create.
        :param tuple index_attr_list: |index_attr_list|

        :Examples:
            :ref:`example-create-table-from-df`
        """

        self.create_table_from_tabledata(
            TableData.from_dataframe(dataframe=dataframe, table_name=table_name),
            index_attr_list=index_attr_list,
        )

    def rollback(self):
        """
        .. seealso:: :py:meth:`sqlite3.Connection.rollback`
        """

        try:
            self.check_connection()
        except NullDatabaseConnectionError:
            return

        logger.debug("rollback: path='{}'".format(self.database_path))

        self.connection.rollback()

    def commit(self):
        """
        .. seealso:: :py:meth:`sqlite3.Connection.commit`
        """

        try:
            self.check_connection()
        except NullDatabaseConnectionError:
            return

        logger.debug("commit: path='{}'".format(self.database_path))

        self.connection.commit()

    def close(self):
        """
        Commit and close the connection.

        .. seealso:: :py:meth:`sqlite3.Connection.close`
        """

        if self.__delayed_connection_path and self.__connection is None:
            self.__initialize_connection()
            return

        try:
            self.check_connection()
        except (SystemError, NullDatabaseConnectionError):
            return

        logger.debug("close connection to a SQLite database: path='{}'".format(self.database_path))

        self.commit()
        self.connection.close()
        self.__initialize_connection()

    def __initialize_connection(self):
        self.__database_path = None
        self.__connection = None
        self.__mode = None
        self.__delayed_connection_path = None

        self.__dict_query_count = {}
        self.__dict_query_totalexectime = {}

    @staticmethod
    def __validate_db_path(database_path):
        if typepy.is_null_string(database_path):
            raise ValueError("null path")

        if database_path == MEMORY_DB_NAME:
            return

        try:
            pathvalidate.validate_filename(os.path.basename(database_path))
        except AttributeError:
            raise TypeError("database path must be a string: actual={}".format(type(database_path)))

    def __verify_db_file_existence(self, database_path):
        """
        :raises SimpleSQLite.OperationalError: If unable to open database file.
        """

        self.__validate_db_path(database_path)
        if not os.path.isfile(os.path.realpath(database_path)):
            raise IOError("file not found: " + database_path)

        try:
            connection = sqlite3.connect(database_path)
        except sqlite3.OperationalError as e:
            raise OperationalError(e)

        connection.close()

    def __delayed_connect(self):
        if self.__delayed_connection_path is None:
            return False

        # save and clear delayed_connection_path to avoid infinite recursion before
        # calling the connect method
        connection_path = self.__delayed_connection_path
        self.__delayed_connection_path = None

        self.connect(connection_path, self.__mode)

        return True

    @staticmethod
    def __validate_attr_name_list(attr_name_list):
        if typepy.is_empty_sequence(attr_name_list):
            raise NameValidationError("attribute name list is empty")

        for attr_name in attr_name_list:
            pathvalidate.validate_sqlite_attr_name(attr_name)

    @staticmethod
    def __extract_list_from_fetch_result(result):
        """
        :params tuple result: Return value from a Cursor.fetchall()
        :rtype: list
        """

        return [record[0] for record in result]

    def __extract_attr_desc_list_from_tabledata(self, table_data):
        attr_description_list = []
        for col, value_type in sorted(
            six.iteritems(self.__extract_col_type_from_tabledata(table_data))
        ):
            attr_name = table_data.header_list[col]
            attr_description_list.append("{} {:s}".format(Attr(attr_name), value_type))

        return attr_description_list

    @staticmethod
    def __extract_col_type_from_tabledata(table_data):
        """
        Extract data type name for each column as SQLite names.

        :param tabledata.TableData table_data:
        :return: { column_number : column_data_type }
        :rtype: dictionary
        """

        typename_table = {
            typepy.Typecode.INTEGER: "INTEGER",
            typepy.Typecode.REAL_NUMBER: "REAL",
            typepy.Typecode.STRING: "TEXT",
        }

        dp_extractor = dataproperty.DataPropertyExtractor()
        col_dp_list = dp_extractor.to_column_dp_list(table_data.value_dp_matrix)

        return dict(
            [
                [col_idx, typename_table.get(col_dp.typecode, "TEXT")]
                for col_idx, col_dp in enumerate(col_dp_list)
            ]
        )

    def __create_table_from_tabledata(self, table_data, index_attr_list=None):
        self.validate_access_permission(["w", "a"])

        logger.debug("__create_table_from_tabledata: {}".format(table_data))

        if table_data.is_empty():
            raise ValueError("input table_data is empty: {}".format(table_data))

        table_data = SQLiteTableDataSanitizer(
            table_data, dup_col_handler=self.dup_col_handler
        ).normalize()

        self.create_table(
            table_data.table_name, self.__extract_attr_desc_list_from_tabledata(table_data)
        )
        self.insert_many(table_data.table_name, table_data.value_matrix)
        if typepy.is_not_empty_sequence(index_attr_list):
            self.create_index_list(table_data.table_name, AttrList.sanitize(index_attr_list))
        self.commit()


def connect_memdb():
    """
    :return: Instance of an in memory database.
    :rtype: SimpleSQLite

    :Example:
        :ref:`example-connect-sqlite-db-mem`
    """

    return SimpleSQLite(MEMORY_DB_NAME, "w")


def connect_sqlite_memdb():
    # [Deprecated]
    return connect_memdb()
