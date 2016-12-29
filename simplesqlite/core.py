# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import
from __future__ import unicode_literals
import logging
import os
import re
import sqlite3

import dataproperty as dp
from mbstrdecoder import MultiByteStrDecoder
import pathvalidate
import pytablereader as ptr
import six
from six.moves import range

from .sqlquery import SqlQuery
from .converter import RecordConvertor
from ._error import (
    AttributeNotFoundError,
    NullDatabaseConnectionError,
    TableNotFoundError,
    InvalidTableNameError,
    InvalidAttributeNameError,
    OperationalError
)
from ._func import (
    connect_sqlite_db_mem,
    validate_table_name,
    MEMORY_DB_NAME
)


class SimpleSQLite(object):
    """
    Wrapper class of |sqlite3| module.

    :param str database_path: File path of the database to be connected.
    :param str mode: Open mode.
    :param bool profile:
        Recording SQL query execution time profile, if the value is |True|.

    .. seealso::

        :py:meth:`.connect`
        :py:meth:`.get_profile`
    """

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

        return self.__database_path

    @property
    def connection(self):
        """
        :return: |Connection| instance of the connected database.
        :rtype: sqlite3.Connection
        """

        return self.__connection

    @property
    def mode(self):
        """
        :return: Connection mode: ``"r"``/``"w"``/``"a"``.
        :rtype: str

        .. seealso:: :py:meth:`.connect`
        """

        return self.__mode

    def __init__(
            self, database_path, mode="a",
            is_create_table_config=True, profile=False):
        self.__initialize_connection()
        self.__is_profile = profile
        self.__is_create_table_config = is_create_table_config

        self.connect(database_path, mode)

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

        :Examples:

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

            .. parsed-literal::

                ---- connected to a database ----
                ---- disconnected from a database ----
                null database connection
        """

        if self.connection is None:
            raise NullDatabaseConnectionError(
                "null database connection")

        if dp.is_empty_string(self.database_path):
            raise NullDatabaseConnectionError(
                "null database file path")

    def connect(self, database_path, mode="a"):
        """
        Connect to a SQLite dtabase.

        :param str database_path:
            Path to the SQLite database file to be connected.
        :param str mode:
            ``"r"``: Open for read only.
            ``"w"``: Open for read/write.
            Delete existing tables when connecting.
            ``"a"``: Open for read/write. Append to the existing tables.
        :raises ValueError:
            If ``database_path`` is invalid or |attr_mode| is invalid.
        :raises simplesqlite.OperationalError:
            If unable to open the database file.
        """

        self.close()

        if mode == "r":
            self.__verify_sqlite_db_file(database_path)
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

        if mode != "w":
            return

        for table in self.get_table_name_list():
            self.drop_table(table)

    def execute_query(self, query, caller=None):
        """
        Send arbitrary SQLite query to the database.

        :param str query: Query to be executed.
        :param tuple caller:
            Caller information.
            Expects the return value of :py:meth:`logging.Logger.findCaller`.
        :return: Result of the query execution.
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
        if dp.is_empty_string(query):
            return None

        if self.__is_profile:
            exec_start_time = time.time()

        try:
            result = self.connection.execute(query)
        except sqlite3.OperationalError as e:
            if caller is None:
                caller = logging.getLogger().findCaller()
            file_path, line_no, func_name = caller[:3]
            message_list = [
                "failed to execute query at %{:s}({:d}) {:s}".format(
                    file_path, line_no, func_name),
                "  - query: {}".format(query),
                "  - msg:   {}".format(str(e)),
                "  - db:    {}".format(self.database_path),
            ]
            raise OperationalError(os.linesep.join(message_list))

        if self.__is_profile:
            self.__dict_query_count[query] = (
                self.__dict_query_count.get(query, 0) + 1)

            elapse_time = time.time() - exec_start_time
            self.__dict_query_totalexectime[query] = (
                self.__dict_query_totalexectime.get(query, 0) + elapse_time)

        return result

    def select(self, select, table_name, where=None, extra=None):
        """
        Send a SELECT query to the database.

        :param str select: Attribute for the SELECT query.
        :param str table_name: Table name of executing the query.
        :return: Result of the query execution.
        :rtype: sqlite3.Cursor
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        .. seealso::

            :py:meth:`.sqlquery.SqlQuery.make_select`
        """

        self.verify_table_existence(table_name)
        query = SqlQuery.make_select(select, table_name, where, extra)

        return self.execute_query(query, logging.getLogger().findCaller())

    def insert(self, table_name, insert_record):
        """
        Send an INSERT query to the database.

        :param str table_name: Table name of executing the query.
        :param insert_record: Record to be inserted.
        :type insert_record: |dict|/|namedtuple|/|list|/|tuple|
        :raises IOError: |raises_write_permission|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Examples:

            :ref:`example-insert-records`
        """

        self.insert_many(table_name, [insert_record])

    def insert_many(self, table_name, insert_record_list):
        """
        Send an INSERT query with multiple records to the database.

        :param str table: Table name of executing the query.
        :param insert_record: Records to be inserted.
        :type insert_record: |dict|/|namedtuple|/|list|/|tuple|
        :raises IOError: |raises_write_permission|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Examples:

            :ref:`example-insert-records`

        .. seealso:: :py:meth:`.sqlquery.SqlQuery.make_insert`
        """

        self.validate_access_permission(["w", "a"])
        self.verify_table_existence(table_name)

        if dp.is_empty_sequence(insert_record_list):
            return

        record_list = RecordConvertor.to_record_list(
            self.get_attr_name_list(table_name), insert_record_list)

        query = SqlQuery.make_insert(table_name, record_list[0])

        try:
            self.connection.executemany(query, record_list)
        except sqlite3.OperationalError as e:
            caller = logging.getLogger().findCaller()
            file_path, line_no, func_name = caller[:3]
            raise OperationalError(
                "{:s}({:d}) {:s}: failed to execute query:\n".format(
                    file_path, line_no, func_name) +
                "  query={}\n".format(query) +
                "  msg='{}'\n".format(str(e)) +
                "  db={}\n".format(self.database_path) +
                "  records={}\n".format(record_list[:2])
            )

    def update(self, table_name, set_query, where=None):
        """
        Send an UPDATE query to the database.

        :param str table_name: Table name of executing the query.
        :param str set_query:
        :raises IOError: |raises_write_permission|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        .. seealso::

            :py:meth:`.sqlquery.SqlQuery.make_update`
        """

        self.validate_access_permission(["w", "a"])
        self.verify_table_existence(table_name)

        query = SqlQuery.make_update(table_name, set_query, where)

        return self.execute_query(query, logging.getLogger().findCaller())

    def get_total_changes(self):
        """
        .. seealso::

            :py:attr:`sqlite3.Connection.total_changes`
        """

        self.check_connection()

        return self.connection.total_changes

    def get_value(self, select, table_name, where=None, extra=None):
        """
        Get a value from the table.

        :param str select: Attribute for SELECT query
        :param str table_name: Table name of executing the query.
        :return: Result of execution of the query.
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        .. seealso::

            :py:meth:`.sqlquery.SqlQuery.make_select`
        """

        self.verify_table_existence(table_name)

        query = SqlQuery.make_select(select, table_name, where, extra)
        result = self.execute_query(query, logging.getLogger().findCaller())
        if result is None:
            return None

        fetch = result.fetchone()
        if fetch is None:
            return None

        return fetch[0]

    def get_table_name_list(self):
        """
        :return: List of table names in the database.
        :rtype: list
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Examples:

            .. code:: python

                from simplesqlite import SimpleSQLite

                con = SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name="hoge",
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])
                print(con.get_table_name_list())

            .. parsed-literal::

                [u'hoge']
        """

        self.check_connection()

        query = "SELECT name FROM sqlite_master WHERE TYPE='table'"
        result = self.execute_query(query, logging.getLogger().findCaller())
        if result is None:
            return []

        return self.__get_list_from_fetch(result.fetchall())

    def get_attribute_name_list(self, table_name):
        # alias of the get_attr_name_list method.
        # this method will be deleted in the future.

        return self.get_attr_name_list(table_name)

    def get_attr_name_list(self, table_name):
        """
        :return: List of attribute names in the table.
        :rtype: list
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        :Examples:

            .. code:: python

                import simplesqlite

                table_name = "sample_table"
                con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name,
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])

                print(con.get_attr_name_list(table_name))

                try:
                    print(con.get_attr_name_list("not_existing"))
                except simplesqlite.TableNotFoundError as e:
                    print(e)

            .. parsed-literal::

                ['attr_a', 'attr_b']
                'not_existing' table not found in /tmp/sample.sqlite
        """

        self.verify_table_existence(table_name)

        query = "SELECT * FROM '{:s}'".format(table_name)
        result = self.execute_query(query, logging.getLogger().findCaller())

        return [
            MultiByteStrDecoder(attr).unicode_str
            for attr in self.__get_list_from_fetch(result.description)
        ]

    def get_attr_type(self, table_name):
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
                SqlQuery.to_value_str(table_name)))
        query = result.fetchone()[0]
        match = re.search("[(].*[)]", query)

        def get_entry(item_list):
            key = " ".join(item_list[:-1])
            value = item_list[-1]

            return [key, value]

        return dict([
            get_entry(item.split(" "))
            for item in match.group().strip("()").split(", ")
        ])

    def get_attr_type_list(self, table_name):
        """
        :return: List of attribute names in the table.
        :rtype: list
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.OperationalError: |raises_operational_error|

        .. warning::

            This method will be deleted in the future.
            Use :py:meth:`.get_attr_type` instead.
        """

        self.verify_table_existence(table_name)

        attribute_name_list = self.get_attr_name_list(table_name)
        query = "SELECT DISTINCT {:s} FROM '{:s}'".format(
            ",".join([
                "TYPEOF({:s})".format(SqlQuery.to_attr_str(attribute))
                for attribute in attribute_name_list]),
            table_name)
        result = self.execute_query(query, logging.getLogger().findCaller())

        return result.fetchone()

    def get_num_records(self, table_name, where=None):
        """
        :param str table_name: Table name to get records.
        :param str where: Where clause of the query.
        :return: Number of records in the table.
        :rtype: int
        """

        return self.get_value(
            select="COUNT(*)", table_name=table_name, where=where)

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

        :Examples:

            :ref:`example-get-profile`
        """

        from collections import namedtuple

        profile_table_name = "sql_profile"

        value_matrix = [
            [query, execute_time, self.__dict_query_count.get(query, 0)]
            for query, execute_time
            in six.iteritems(self.__dict_query_totalexectime)
        ]
        attr_name_list = ("sql_query", "cumulative_time", "count")
        con_tmp = connect_sqlite_db_mem()
        try:
            con_tmp.create_table_with_data(
                profile_table_name,
                attr_name_list,
                data_matrix=value_matrix)
        except ValueError:
            return []

        try:
            result = con_tmp.select(
                select="{:s},SUM({:s}),SUM({:s})".format(*attr_name_list),
                table_name=profile_table_name,
                extra="GROUP BY {:s} ORDER BY {:s} DESC LIMIT {:d}".format(
                    attr_name_list[0], attr_name_list[1], profile_count))
        except sqlite3.OperationalError:
            return []
        if result is None:
            return []

        SqliteProfile = namedtuple(
            "SqliteProfile", " ".join(attr_name_list))

        return [SqliteProfile(*profile) for profile in result.fetchall()]

    def get_sqlite_master(self):
        """
        Get sqlite_master table information as a list of dictionaries.

        :return: sqlite_master table information.
        :rtype: list
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|

        :Examples:

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

                print(json.dumps(con.get_sqlite_master(), indent=4))

            .. parsed-literal::

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

        old_row_factory = self.connection.row_factory
        self.connection.row_factory = sqlite3.Row

        sqlite_master_list = []
        result = self.execute_query("select * from sqlite_master")
        for item in result.fetchall():
            sqlite_master_list.append(
                dict([[key, item[key]] for key in item.keys()]))

        self.connection.row_factory = old_row_factory

        return sqlite_master_list

    def has_table(self, table_name):
        """
        :param str table_name: Table name to be tested.
        :return: |True| if the database has the table.
        :rtype: bool

        :Examples:

            .. code:: python

                from simplesqlite import SimpleSQLite

                con = SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name="hoge",
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])

                print(con.has_table("hoge"))
                print(con.has_table("not_existing"))

            .. parsed-literal::

                True
                False
        """

        try:
            validate_table_name(table_name)
        except InvalidTableNameError:
            return False

        return table_name in self.get_table_name_list()

    def has_attribute(self, table_name, attribute_name):
        """
        :param str table_name: Table name that exists attribute.
        :param str attribute_name: Attribute name to be tested.
        :return: |True| if the table has the attribute.
        :rtype: bool
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|

        :Examples:

            .. code:: python

                import simplesqlite

                table_name = "sample_table"
                con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name=table_name,
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])

                print(con.has_attribute(table_name, "attr_a"))
                print(con.has_attribute(table_name, "not_existing"))
                try:
                    print(con.has_attribute("not_existing", "attr_a"))
                except simplesqlite.TableNotFoundError as e:
                    print(e)

            .. parsed-literal::

                True
                False
                'not_existing' table not found in /tmp/sample.sqlite
        """

        self.verify_table_existence(table_name)

        if dp.is_empty_string(attribute_name):
            return False

        return attribute_name in self.get_attr_name_list(table_name)

    def has_attribute_list(self, table_name, attribute_name_list):
        """
        :param str table_name: Table name that exists attribute.
        :param str attribute_name_list: Attribute names to be tested.
        :return: |True| if the table has all of the attribute.
        :rtype: bool
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|

        :Examples:

            .. code:: python

                import simplesqlite

                table_name = "sample_table"
                con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
                con.create_table_from_data_matrix(
                    table_name=table_name,
                    attr_name_list=["attr_a", "attr_b"],
                    data_matrix=[[1, "a"], [2, "b"]])

                print(con.has_attribute_list(table_name, ["attr_a"]))
                print(con.has_attribute_list(table_name, ["attr_a", "attr_b"]))
                print(con.has_attribute_list(
                    table_name, ["attr_a", "attr_b", "not_existing"]))
                try:
                    print(con.has_attribute("not_existing", ["attr_a"]))
                except simplesqlite.TableNotFoundError as e:
                    print(e)

            .. parsed-literal::

                True
                True
                False
                'not_existing' table not found in /tmp/sample.sqlite
        """

        if dp.is_empty_sequence(attribute_name_list):
            return False

        not_exist_field_list = [
            attribute_name
            for attribute_name in attribute_name_list
            if not self.has_attribute(table_name, attribute_name)
        ]

        if len(not_exist_field_list) > 0:
            return False

        return True

    def verify_table_existence(self, table_name):
        """
        :param str table_name: Table name to be tested.
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        :raises simplesqlite.InvalidTableNameError:
            |raises_validate_table_name|

        :Examples:

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

            .. parsed-literal::

                'not_existing' table not found in /tmp/sample.sqlite
        """

        validate_table_name(table_name)

        if self.has_table(table_name):
            return

        raise TableNotFoundError(
            "'{}' table not found in {}".format(
                table_name, self.database_path))

    def verify_attr_existence(self, table_name, attribute_name):
        """
        :param str table_name: Table name that exists attribute.
        :param str attribute_name: Attribute name to be tested.
        :raises simplesqlite.AttributeNotFoundError:
            If attribute not found in the table
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|

        :Examples:

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

            .. parsed-literal::

                'not_existing' attribute not found in 'sample_table' table
                'not_existing' table not found in /tmp/sample.sqlite
        """

        self.verify_table_existence(table_name)

        if self.has_attribute(table_name, attribute_name):
            return

        raise AttributeNotFoundError(
            "'{}' attribute not found in '{}' table".format(
                attribute_name, table_name))

    def drop_table(self, table_name):
        """
        :param str table_name: Table name to drop.
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises IOError: |raises_write_permission|
        """

        self.validate_access_permission(["w", "a"])

        if self.has_table(table_name):
            query = "DROP TABLE IF EXISTS '{:s}'".format(table_name)
            self.execute_query(query, logging.getLogger().findCaller())
            self.commit()

    def create_table(self, table_name, attribute_description_list):
        """
        :param str table_name: Table name to create.
        :param list attribute_description_list: List of table description.
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises IOError: |raises_write_permission|
        """

        self.validate_access_permission(["w", "a"])

        table_name = table_name.strip()
        if self.has_table(table_name):
            return True

        query = "CREATE TABLE IF NOT EXISTS '{:s}' ({:s})".format(
            table_name, ", ".join(attribute_description_list))
        if self.execute_query(query, logging.getLogger().findCaller()) is None:
            return False

        return True

    def create_index(self, table_name, attribute_name):
        """
        :param str table_name:
            Table name that contains the attribute to be indexed.
        :param str attribute_name: Attribute name to create index.
        :raises IOError: |raises_write_permission|
        :raises simplesqlite.NullDatabaseConnectionError:
            |raises_check_connection|
        :raises simplesqlite.TableNotFoundError:
            |raises_verify_table_existence|
        """

        self.verify_table_existence(table_name)
        self.validate_access_permission(["w", "a"])

        index_name = "{:s}_{:s}_index".format(
            SqlQuery.sanitize(table_name), SqlQuery.sanitize(attribute_name))
        if attribute_name.find("'") != -1:
            query_format = 'CREATE INDEX IF NOT EXISTS {:s} ON {:s}("{:s})'
        else:
            query_format = "CREATE INDEX IF NOT EXISTS {:s} ON {:s}('{:s}')"

        query = query_format.format(
            index_name,
            SqlQuery.to_table_str(table_name),
            attribute_name)
        self.execute_query(query, logging.getLogger().findCaller())

    def create_index_list(self, table_name, attribute_name_list):
        """
        :param str table_name: Table name that exists attribute.
        :param list attribute_name_list:
            List of attribute names to create indices.
            Ignore attributes that not existing in the table.

        .. seealso:: :py:meth:`.create_index`
        """

        self.validate_access_permission(["w", "a"])

        if dp.is_empty_sequence(attribute_name_list):
            return

        table_attr_set = set(self.get_attr_name_list(table_name))
        index_attr_set = set(attribute_name_list)

        for attribute in list(table_attr_set.intersection(index_attr_set)):
            self.create_index(table_name, attribute)

    def __sanitize_attr_name_list(self, attr_name_list):
        return [
            SqlQuery.sanitize_attr(attr_name)
            for attr_name in attr_name_list
        ]

    def create_table_with_data(
            self, table_name, attribute_name_list, data_matrix,
            index_attribute_list=None):
        """
        Create a table if not exists. And insert data into the created table.

        Alias of :py:meth:`~.create_table_from_data_matrix`.

        :param str table_name: Table name to create.
        :param list attribute_name_list: List of attribute names of the table.
        :param data_matrix: Data to be inserted into the table.
        :type data_matrix: List of |dict|/|namedtuple|/|list|/|tuple|
        :param tuple index_attribute_list:
            List of attribute names of create indices.
        :raises simplesqlite.InvalidTableNameError:
            |raises_validate_table_name|
        :raises simplesqlite.InvalidAttributeNameError:
            |raises_validate_attr_name|
        :raises ValueError: If the ``data_matrix`` is empty.

        .. seealso::

            :py:meth:`.create_table`
            :py:meth:`.insert_many`
            :py:meth:`.create_index_list`
        """

        self.create_table_from_data_matrix(
            table_name, attribute_name_list, data_matrix, index_attribute_list)

    def create_table_from_data_matrix(
            self, table_name, attr_name_list, data_matrix,
            index_attr_list=None):
        """
        Create a table if not exists. And insert data into the created table.

        :param str table_name: Table name to create.
        :param list attr_name_list: List of attribute names of the table.
        :param data_matrix: Data to be inserted into the table.
        :type data_matrix: List of |dict|/|namedtuple|/|list|/|tuple|
        :param tuple index_attr_list:
            List of attribute names of create indices.
        :raises simplesqlite.InvalidTableNameError:
            |raises_validate_table_name|
        :raises simplesqlite.InvalidAttributeNameError:
            |raises_validate_attr_name|
        :raises ValueError: If the ``data_matrix`` is empty.

        :Examples:

            :ref:`example-create-table-from-data-matrix`

        .. seealso::

            :py:meth:`.create_table`
            :py:meth:`.insert_many`
            :py:meth:`.create_index_list`
        """

        self.__create_table_from_tabledata(
            ptr.TableData(table_name, attr_name_list, data_matrix),
            index_attr_list)

    def create_table_from_tabledata(self, tabledata, index_attr_list=None):
        """
        Create a table from :py:class:`pytablereader.TableData`.

        :param TableData tabledata: Table data to create.

        .. seealso::

            :py:meth:`.create_table_from_data_matrix`
        """

        self.__create_table_from_tabledata(tabledata, index_attr_list)

    def __create_table_from_tabledata(
            self, tabledata, index_attr_list=None):

        self.validate_access_permission(["w", "a"])
        validate_table_name(tabledata.table_name)

        attr_name_list = self.__sanitize_attr_name_list(tabledata.header_list)
        try:
            self.__validate_attr_name_list(attr_name_list)
        except pathvalidate.ReservedNameError:
            pass

        if dp.is_empty_sequence(tabledata.value_matrix):
            raise ValueError("input data is null: '{} ({})'".format(
                tabledata.table_name, ", ".join(attr_name_list)))

        self.__verify_value_matrix(attr_name_list, tabledata.value_matrix)

        self.create_table(
            tabledata.table_name,
            self.__get_attr_desc_list(
                attr_name_list, tabledata.value_matrix))
        self.insert_many(tabledata.table_name, tabledata.value_matrix)
        if dp.is_not_empty_sequence(index_attr_list):
            self.create_index_list(
                tabledata.table_name,
                self.__sanitize_attr_name_list(index_attr_list))
        self.commit()

    def create_table_from_csv(
            self, csv_source, table_name="",
            attribute_name_list=(),
            delimiter=",", quotechar='"', encoding="utf-8"):
        """
        Create a table from a CSV file/text.

        :param str csv_source: Path to the CSV file or CSV text.
        :param str table_name:
            Table name to create.
            Use csv file basename as the table name if the value is empty.
        :param list attribute_name_list:
            Attribute names of the table.
            Use the first line of the csv file as attribute list
            if attribute_name_list is empty.
        :param str delimiter:
            A one-character string used to separate fields.
        :param str quotechar:
            A one-character string used to quote fields
            containing special characters, such as the delimiter or quotechar,
            or which contain new-line characters.
        :param str encoding: csv file encoding.
        :raises ValueError: If the csv data is invalid.

        :Examples:

            :ref:`example-create-table-from-csv`

        .. seealso::

            :py:meth:`.create_table_from_data_matrix`
            :py:func:`csv.reader`
            :py:meth:`.pytablereader.CsvTableFileLoader.load`
            :py:meth:`.pytablereader.CsvTableTextLoader.load`
        """

        from pytablereader import CsvTableFileLoader
        from pytablereader import CsvTableTextLoader

        loader = CsvTableFileLoader(csv_source)
        if dp.is_not_empty_string(table_name):
            loader.table_name = table_name
        loader.header_list = attribute_name_list
        loader.delimiter = delimiter
        loader.quotechar = quotechar
        loader.encoding = encoding
        try:
            for tabledata in loader.load():
                self.create_table_from_tabledata(tabledata)
            return
        except (ptr.InvalidFilePathError, IOError):
            pass

        loader = CsvTableTextLoader(csv_source)
        if dp.is_not_empty_string(table_name):
            loader.table_name = table_name
        loader.header_list = attribute_name_list
        loader.delimiter = delimiter
        loader.quotechar = quotechar
        loader.encoding = encoding
        for tabledata in loader.load():
            self.create_table_from_tabledata(tabledata)

    def create_table_from_json(self, json_source, table_name=""):
        """
        Create a table from a JSON file/text.

        :param str json_source: Path to the JSON file or JSON text.
        :param str table_name: Table name to create.

        :Examples:

            :ref:`example-create-table-from-json`

        .. seealso::

            :py:meth:`.pytablereader.JsonTableFileLoader.load`
            :py:meth:`.pytablereader.JsonTableTextLoader.load`
        """

        from pytablereader import JsonTableFileLoader
        from pytablereader import JsonTableTextLoader

        loader = JsonTableFileLoader(json_source)
        if dp.is_not_empty_string(table_name):
            loader.table_name = table_name
        try:
            for tabledata in loader.load():
                self.create_table_from_tabledata(tabledata)
            return
        except (ptr.InvalidFilePathError, IOError):
            pass

        loader = JsonTableTextLoader(json_source)
        if dp.is_not_empty_string(table_name):
            loader.table_name = table_name
        for tabledata in loader.load():
            self.create_table_from_tabledata(tabledata)

    def rollback(self):
        """
        .. seealso:: :py:meth:`sqlite3.Connection.rollback`
        """

        try:
            self.check_connection()
        except NullDatabaseConnectionError:
            return

        self.connection.rollback()

    def commit(self):
        """
        .. seealso:: :py:meth:`sqlite3.Connection.commit`
        """

        try:
            self.check_connection()
        except NullDatabaseConnectionError:
            return

        self.connection.commit()

    def close(self):
        """
        Commit and close the connection.

        .. seealso:: :py:meth:`sqlite3.Connection.close`
        """

        try:
            self.check_connection()
        except NullDatabaseConnectionError:
            return

        self.commit()
        self.connection.close()
        self.__initialize_connection()

    @staticmethod
    def __validate_db_path(database_path):
        if dp.is_empty_string(database_path):
            raise ValueError("null path")

        if database_path == MEMORY_DB_NAME:
            return

        pathvalidate.validate_filename(os.path.basename(database_path))

    def __verify_sqlite_db_file(self, database_path):
        """
        :raises sqlite3.OperationalError: If unable to open database file.
        """

        self.__validate_db_path(database_path)
        if not os.path.isfile(os.path.realpath(database_path)):
            raise IOError("file not found: " + database_path)

        try:
            connection = sqlite3.connect(database_path)
        except sqlite3.OperationalError as e:
            raise OperationalError(e)

        connection.close()

    @staticmethod
    def __validate_attr_name_list(attr_name_list):
        if dp.is_empty_sequence(attr_name_list):
            raise InvalidAttributeNameError("attribute name list is empty")

        for attr_name in attr_name_list:
            pathvalidate.validate_sqlite_attr_name(attr_name)

    @staticmethod
    def __verify_value_matrix(field_list, value_matrix):
        """
        :param list/tuple field_list:
        :param list/tuple value_matrix: the list to test.
        :raises ValueError:
        """

        miss_match_idx_list = []

        for list_idx in range(len(value_matrix)):
            if len(field_list) == len(value_matrix[list_idx]):
                continue

            miss_match_idx_list.append(list_idx)

        if len(miss_match_idx_list) == 0:
            return

        sample_miss_match_list = value_matrix[miss_match_idx_list[0]]

        raise ValueError(
            "miss match header length and value length:" +
            "  header: {} {}\n".format(len(field_list), field_list) +
            "  # of miss match line: {} ouf of {}\n".format(
                len(miss_match_idx_list), len(value_matrix)) +
            "  e.g. value at line={}, col-size={}: {}\n".format(
                miss_match_idx_list[0],
                len(sample_miss_match_list), sample_miss_match_list)
        )

    @staticmethod
    def __get_list_from_fetch(result):
        """
        :params tuple result: Return value from a Cursor.fetchall()
        :rtype: list
        """

        return [record[0] for record in result]

    def __initialize_connection(self):
        self.__database_path = None
        self.__connection = None
        self.__cursur = None
        self.__mode = None

        self.__dict_query_count = {}
        self.__dict_query_totalexectime = {}

    def __get_attr_desc_list(self, attr_name_list, data_matrix):
        attr_description_list = []
        for col, value_type in sorted(
                six.iteritems(self.__get_column_valuetype(data_matrix))):
            attr_name = attr_name_list[col]
            attr_description_list.append("{:s} {:s}".format(
                SqlQuery.to_attr_str(attr_name), value_type))

        return attr_description_list

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

        if dp.is_empty_string(self.mode):
            raise ValueError("mode is not set")

        if self.mode not in valid_permission_list:
            raise IOError(
                "invalid access: expected-mode='{}', current-mode='{}'".format(
                    "' or '".join(valid_permission_list), self.mode))

    @staticmethod
    def __get_column_valuetype(data_matrix):
        """
        Get value type for each column.

        :param list/tuple data_matrix:
        :return: { column_number : value_type }
        :rtype: dictionary
        """

        typename_table = {
            dp.Typecode.INTEGER: "INTEGER",
            dp.Typecode.FLOAT: "REAL",
            dp.Typecode.STRING: "TEXT",
        }

        prop_extractor = dp.PropertyExtractor()
        prop_extractor.data_matrix = data_matrix
        col_prop_list = prop_extractor.extract_column_property_list()

        return dict([
            [col, typename_table.get(col_prop.typecode, "TEXT")]
            for col, col_prop in enumerate(col_prop_list)
        ])
