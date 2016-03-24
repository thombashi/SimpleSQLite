# encoding: utf-8

'''
@author: Tsuyoshi Hombashi
'''


from __future__ import absolute_import
import logging
import os
import sqlite3
import sys

import dataproperty
import pathvalidate
import six
from six.moves import range

import simplesqlite
from .sqlquery import SqlQuery


class SimpleSQLite(object):
    """
    Wrapper class of sqlite3 module.
    """

    class TableConfiguration:
        TABLE_NAME = "__table_configuration__"
        ATTRIBUTE_NAME_LIST = [
            "table_name", "attribute_name", "value_type", "has_index",
        ]

    @property
    def database_path(self):
        """
        :return: File path of the connected database.
        :rtype: str
        """

        return self.__database_path

    @property
    def connection(self):
        """
        :return: Connection instance of the connected database.
        :rtype: sqlite3.Connection
        """

        return self.__connection

    @property
    def mode(self):
        """
        :return: Connection mode: "r"/"w"/"a".
        :rtype: str
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
        :return: ``True`` if the connection to a database is valid.
        :rtype: bool
        """

        try:
            self.check_connection()
        except simplesqlite.NullDatabaseConnectionError:
            return False

        return True

    def check_connection(self):
        """
        :raises NullDatabaseConnectionError:
            If not connected to a SQLite database file.
        """

        if self.connection is None:
            raise simplesqlite.NullDatabaseConnectionError(
                "null database connection")

        if dataproperty.is_empty_string(self.database_path):
            raise simplesqlite.NullDatabaseConnectionError(
                "null database file path")

    def connect(self, database_path, mode="a"):
        """
        :param str database_path: File path of the database to be connected.
        :param str mode:
            ``"r"``: Open for read only.
            ``"w"``: Open for read/write. Delete existing tables.
            ``"a"``: Open for read/write. Append to the existing tables.
        :raises ValueError: If ``database_path`` is invalid or ``mode`` is invalid.
        :raises sqlite3.OperationalError: If unable to open the database file.
        """

        self.close()

        if mode == "r":
            self.__verify_sqlite_db_file(database_path)
        elif mode in ["w", "a"]:
            self.__validate_db_path(database_path)
        else:
            raise ValueError("unknown connection mode: " + mode)

        self.__database_path = os.path.realpath(database_path)
        self.__connection = sqlite3.connect(database_path)
        self.__mode = mode

        if mode != "w":
            return

        for table in self.get_table_name_list():
            self.drop_table(table)

    def execute_query(self, query, caller=None):
        """
        Execute arbitrary SQLite query.

        :param str query: Query to be executed.
        :param str tuple:
            Caller information.
            Retuen value of Logger.findCaller().
        :return: Result of the query execution.
        :rtype: sqlite3.Cursor

        :raises sqlite3.OperationalError:
            If failed to execute query.

        .. seealso::

            :py:meth:`.check_connection`
        """

        import time

        self.check_connection()
        if dataproperty.is_empty_string(query):
            return None

        if self.__is_profile:
            exec_start_time = time.time()

        try:
            result = self.connection.execute(query)
        except sqlite3.OperationalError:
            _, e, _ = sys.exc_info()  # for python 2.5 compatibility
            if caller is None:
                caller = logging.getLogger().findCaller()
            file_path, line_no, func_name = caller[:3]
            message_list = [
                "failed to execute query at %s(%d) %s" % (
                    file_path, line_no, func_name),
                "  - query: %s" % (query),
                "  - msg:   %s" % (e),
                "  - db:    %s" % (self.database_path),
            ]
            raise sqlite3.OperationalError(os.linesep.join(message_list))

        if self.__is_profile:
            self.__dict_query_count[query] = (
                self.__dict_query_count.get(query, 0) + 1)

            elapse_time = time.time() - exec_start_time
            self.__dict_query_totalexectime[query] = (
                self.__dict_query_totalexectime.get(query, 0) + elapse_time)

        return result

    def select(self, select, table_name, where=None, extra=None):
        """
        Execute SELCT query.

        :return: Result of the query execution.
        :rtype: sqlite3.Cursor

        .. seealso::

            :py:meth:`.verify_table_existence`
            :py:meth:`.sqlquery.SqlQuery.make_select`
            :py:meth:`.execute_query`
        """

        self.verify_table_existence(table_name)
        query = SqlQuery.make_select(select, table_name, where, extra)

        return self.execute_query(query, logging.getLogger().findCaller())

    def insert(self, table_name, insert_record):
        """
        Execute INSERT query.

        :param str table_name: Table name of execute query
        :param dict/namedtuple/list/tuple insert_record: Record to be inserted

        :raises ValueError: If database connection is invalid.
        :raises IOError: If open mode is neither ``"w"`` nor ``"a"``.

        .. seealso::

            :py:meth:`.check_connection`
            :py:meth:`.sqlquery.SqlQuery.make_insert`
            :py:meth:`.execute_query`
        """

        self.validate_access_permission(["w", "a"])
        self.verify_table_existence(table_name)

        query = SqlQuery.make_insert(table_name, self.__to_record(
            self.get_attribute_name_list(table_name), insert_record))
        self.execute_query(query, logging.getLogger().findCaller())

    def insert_many(self, table_name, insert_record_list):
        """
        Execute INSERT query for multiple records.

        :param str table: Table name of execute query
        :param dict/namedtuple/list/tuple insert_record: Record to be inserted

        :raises ValueError: If database connection is invalid.
        :raises IOError: If open mode is neither ``"w"`` nor ``"a"``.
        :raises sqlite3.OperationalError: If failed to execute query.

        .. seealso::

            :py:meth:`.check_connection`
            :py:meth:`.verify_table_existence`
            :py:meth:`.sqlquery.SqlQuery.make_insert`
        """

        self.validate_access_permission(["w", "a"])
        self.verify_table_existence(table_name)

        if dataproperty.is_empty_list_or_tuple(insert_record_list):
            return

        record_list = self.__to_data_matrix(
            self.get_attribute_name_list(table_name), insert_record_list)

        query = SqlQuery.make_insert(
            table_name, record_list[0], is_insert_many=True)

        try:
            self.connection.executemany(query, record_list)
        except sqlite3.OperationalError:
            _, e, _ = sys.exc_info()  # for python 2.5 compatibility
            caller = logging.getLogger().findCaller()
            file_path, line_no, func_name = caller[:3]
            raise sqlite3.OperationalError(
                "%s(%d) %s: failed to execute query:\n" % (
                    file_path, line_no, func_name) +
                "  query=%s\n" % (query) +
                "  msg='%s'\n" % (str(e)) +
                "  db=%s\n" % (self.database_path) +
                "  records=%s\n" % (record_list[:2])
            )

    def update(self, table_name, set_query, where=None):
        """
        Execute UPDATE query.

        :raises ValueError: If database connection is invalid.
        :raises IOError: If open mode is neither ``"w"`` nor ``"a"``.
        :raises sqlite3.OperationalError:
            If failed to execute query.

        .. seealso::

            :py:meth:`.check_connection`
            :py:meth:`.verify_table_existence`
            :py:meth:`.sqlquery.SqlQuery.make_update`
            :py:meth:`.execute_query`
        """

        self.validate_access_permission(["w", "a"])
        self.verify_table_existence(table_name)

        query = SqlQuery.make_update(table_name, set_query, where)

        return self.execute_query(query, logging.getLogger().findCaller())

    def get_total_changes(self):
        """
        .. seealso::

            :py:meth:`sqlite3.Connection.total_changes`
        """

        self.check_connection()

        return self.connection.total_changes

    def get_value(self, select, table_name, where=None, extra=None):
        """
        Get a value from the table.

        :return: Result of execution of query.

        .. seealso::

            :py:meth:`.sqlquery.SqlQuery.make_select`
            :py:meth:`.execute_query`
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
        :return: Table name list in the database.
        :rtype: list

        .. seealso::

            :py:meth:`.check_connection`
            :py:meth:`.execute_query`
        """

        self.check_connection()

        query = "SELECT name FROM sqlite_master WHERE TYPE='table'"
        result = self.execute_query(query, logging.getLogger().findCaller())
        if result is None:
            return []

        return self.__get_list_from_fetch(result.fetchall())

    def get_attribute_name_list(self, table_name):
        """
        :return: Attribute name list in the table.
        :rtype: list

        :raises TableNotFoundError:
            If ``tablename`` table not found in the database.

        .. seealso::

            :py:meth:`.verify_table_existence`
            :py:meth:`.execute_query`
        """

        self.verify_table_existence(table_name)

        query = "SELECT * FROM '%s'" % (table_name)
        result = self.execute_query(query, logging.getLogger().findCaller())

        return self.__get_list_from_fetch(result.description)

    def get_attribute_type_list(self, table_name):
        """
        :return: Attribute type list in the table.
        :rtype: list

        :raises TableNotFoundError:
            If ``tablename`` table not found in the database.

        .. seealso::

            :py:meth:`.verify_table_existence`
            :py:meth:`.execute_query`
        """

        self.verify_table_existence(table_name)

        attribute_name_list = self.get_attribute_name_list(table_name)
        query = "SELECT DISTINCT %s FROM '%s'" % (
                ",".join([
                    "TYPEOF(%s)" % (SqlQuery.to_attr_str(attribute))
                    for attribute in attribute_name_list]),
                table_name)
        result = self.execute_query(query, logging.getLogger().findCaller())

        return result.fetchone()

    def get_profile(self, profile_count=50):
        """
        Get profile information of query executions.

        :param int profile_count:
            Number of profile count from the longest execution time query.
        :return:
            Query execution profile information.
        :rtype: (list,list)

        .. seealso::

            :py:meth:`.execute_query`
        """

        TN_SQL_PROFILE = "sql_profile"

        value_matrix = []
        for query, execute_time in six.iteritems(self.__dict_query_totalexectime):
            call_count = self.__dict_query_count.get(query, 0)
            value_list = [query, execute_time, call_count]
            value_matrix.append(value_list)

        attribute_name_list = ["query", "execution_time", "count"]
        con_tmp = simplesqlite.connect_sqlite_db_mem()
        try:
            con_tmp.create_table_with_data(
                TN_SQL_PROFILE,
                attribute_name_list,
                data_matrix=value_matrix)
        except ValueError:
            return [], []

        try:
            result = con_tmp.select(
                select="%s,SUM(%s),SUM(%s)" % (
                    "query", "execution_time", "count"),
                table_name=TN_SQL_PROFILE,
                extra="GROUP BY %s ORDER BY %s DESC LIMIT %d" % (
                    "query", "execution_time", profile_count))
        except sqlite3.OperationalError:
            return [], []
        if result is None:
            return [], []

        return attribute_name_list, result.fetchall()

    def has_table(self, table_name):
        """
        :param str table_name: Table name to be tested.
        :return: ``True`` if the database has the table.
        :rtype: bool
        """

        try:
            simplesqlite.validate_table_name(table_name)
        except ValueError:
            return False

        return table_name in self.get_table_name_list()

    def has_attribute(self, table_name, attribute_name):
        """
        :param str table_name: Table name that exists attribute.
        :param str attribute_name: Attribute name to be tested.
        :return: ``True`` if the table has the attribute.
        :rtype: bool

        .. seealso::

            :py:meth:`.verify_table_existence`
        """

        self.verify_table_existence(table_name)

        if dataproperty.is_empty_string(attribute_name):
            return False

        return attribute_name in self.get_attribute_name_list(table_name)

    def has_attribute_list(self, table_name, attribute_name_list):
        """
        :param str table_name: Table name that exists attribute.
        :param str attribute_name: Attribute name to be tested.
        :return: ``True`` if the table has the all of the attribute.
        :rtype: bool

        .. seealso::

            :py:meth:`.has_attribute`
        """

        if dataproperty.is_empty_list_or_tuple(attribute_name_list):
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

        :raises TableNotFoundError: If table not found in the database

        .. seealso::

            :py:func:`simplesqlite.validate_table_name`
        """

        simplesqlite.validate_table_name(table_name)

        if self.has_table(table_name):
            return

        raise simplesqlite.TableNotFoundError(
            "'%s' table not found in %s" % (table_name, self.database_path))

    def verify_attribute_existence(self, table_name, attribute_name):
        """
        :param str table_name: Table name that exists attribute.
        :param str attribute_name: Attribute name to be tested.

        :raises AttributeNotFoundError: If attribute not found in the table

        .. seealso::

            :py:meth:`.verify_table_existence`
        """

        self.verify_table_existence(table_name)

        if self.has_attribute(table_name, attribute_name):
            return

        raise simplesqlite.AttributeNotFoundError(
            "'%s' attribute not found in '%s' table" % (
                attribute_name, table_name))

    def drop_table(self, table_name):
        """
        :param str table_name: Table name to drop.

        :raises ValueError: If database connection is invalid.
        :raises IOError: If open mode is neither ``"w"`` nor ``"a"``.
        """

        self.validate_access_permission(["w", "a"])

        if self.has_table(table_name):
            query = "DROP TABLE IF EXISTS '%s'" % (table_name)
            self.execute_query(query, logging.getLogger().findCaller())
            self.commit()

    def create_table(self, table_name, attribute_description_list):
        """
        :param str table_name: Table name to create.
        :param str attribute_description_list:

        :raises ValueError: If database connection is invalid.
        :raises IOError: If open mode is neither ``"w"`` nor ``"a"``.
        """

        self.validate_access_permission(["w", "a"])

        table_name = table_name.strip()
        if self.has_table(table_name):
            return True

        query = "CREATE TABLE IF NOT EXISTS '%s' (%s)" % (
                table_name, ", ".join(attribute_description_list))
        if self.execute_query(query, logging.getLogger().findCaller()) is None:
            return False

        return True

    def create_index(self, table_name, attribute_name):
        """
        :param str table_name: Table name that exists attribute.
        :param str attribute_name: Attribute name to create index.

        :raises ValueError: If database connection is invalid.
        :raises IOError: If open mode is neither ``"w"`` nor ``"a"``.

        .. seealso::

            :py:meth:`.verify_table_existence`
        """

        self.verify_table_existence(table_name)
        self.validate_access_permission(["w", "a"])

        index_name = "%s_%s_index" % (
            SqlQuery.sanitize(table_name), SqlQuery.sanitize(attribute_name))
        query = "CREATE INDEX IF NOT EXISTS %s ON %s('%s')" % (
                index_name, SqlQuery.to_table_str(table_name), attribute_name)
        self.execute_query(query, logging.getLogger().findCaller())

        if self.__is_create_table_config:
            where_list = [
                SqlQuery.make_where("table_name", table_name),
                SqlQuery.make_where("attribute_name", attribute_name),
            ]
            self.update(
                table_name=self.TableConfiguration.TABLE_NAME,
                set_query="has_index = 1",
                where=" AND ".join(where_list))

    def create_index_list(self, table_name, attribute_name_list):
        """
        :param str table_name: Table name that exists attribute.
        :param list attribute_name_list: Attribute name list to create index.

        .. seealso::

            :py:meth:`.create_index`
        """

        self.validate_access_permission(["w", "a"])

        if dataproperty.is_empty_list_or_tuple(attribute_name_list):
            return

        for attribute in attribute_name_list:
            self.create_index(table_name, attribute)

    def create_table_with_data(
            self, table_name, attribute_name_list, data_matrix,
            index_attribute_list=()):
        """
        Create table if not exists. And insert data to the created table.

        :param str table_name: Table name to create.
        :param list attribute_name_list: Attribute names of the table.
        :param dict/namedtuple/list/tuple data_matrix: Data to be inserted.
        :param tuple index_attribute_list: Attribute name list to create index.
        :raises ValueError: If ``data_matrix`` is empty.

        .. seealso::

            :py:meth:`.create_table`
            :py:meth:`.insert_many`
            :py:meth:`.create_index_list`
        """

        simplesqlite.validate_table_name(table_name)

        self.validate_access_permission(["w", "a"])

        if dataproperty.is_empty_list_or_tuple(data_matrix):
            raise ValueError("input data is null: '%s (%s)'" % (
                table_name, ", ".join(attribute_name_list)))

        data_matrix = self.__to_data_matrix(attribute_name_list, data_matrix)
        self.__verify_value_matrix(attribute_name_list, data_matrix)

        strip_index_attribute_list = list(
            set(attribute_name_list).intersection(set(index_attribute_list)))
        attr_description_list = []

        table_config_matrix = []
        for col, value_type in sorted(
                six.iteritems(self.__get_column_valuetype(data_matrix))):
            attr_name = attribute_name_list[col]
            attr_description_list.append(
                "'%s' %s" % (attr_name, value_type))

            table_config_matrix.append([
                table_name,
                attr_name,
                value_type,
                attr_name in strip_index_attribute_list,
            ])
        self.__create_table_config(table_config_matrix)

        self.create_table(table_name, attr_description_list)
        self.insert_many(table_name, data_matrix)
        self.create_index_list(table_name, strip_index_attribute_list)
        self.commit()

    def create_table_from_csv(
            self, csv_path, table_name="",
            attribute_name_list=[],
            delimiter=",", quotechar='"', encoding="utf-8"):
        """
        Create table from a csv file.

        :param str csv_path: Path to the csv file.
        :param str table_name:
            Table name to create (default="").
            Use csv file base name as the table name if table_name is empty.
        :param list attribute_name_list:
            Attribute names of the table (default=[]).
            Use first line of the csv file as attribute list
            if attribute_name_list is empty.
        :param str delimiter:
            A one-character string used to separate fields. It defaults to `','`.
        :param str quotechar:
            A one-character string used to quote fields
            containing special characters, such as the delimiter or quotechar,
            or which contain new-line characters. It defaults to `'"'`.
        :param str encoding: csv file encoding. It defaults to `'utf-8``

        :raises ValueError:

        .. seealso::

            :py:meth:`.create_table_with_data`
            :py:meth:`csv.reader`
        """

        import csv

        csv_reader = csv.reader(
            open(csv_path, "r"), delimiter=delimiter, quotechar=quotechar)

        data_matrix = [
            [
                six.b(data).decode(encoding, "ignore")
                if not dataproperty.is_float(data) else data
                for data in row
            ]
            for row in csv_reader
        ]

        if dataproperty.is_empty_list_or_tuple(attribute_name_list):
            header_list = data_matrix[0]

            if any([
                dataproperty.is_empty_string(header) for header in header_list
            ]):
                raise ValueError(
                    "the first line include empty string: "
                    "the first line expected to contain header data.")

            data_matrix = data_matrix[1:]
        else:
            header_list = attribute_name_list

        if dataproperty.is_empty_string(table_name):
            # use csv filename as a table name if table_name is a empty string.
            table_name = os.path.splitext(os.path.basename(csv_path))[0]

        self.create_table_with_data(table_name, header_list, data_matrix)

    def rollback(self):
        try:
            self.check_connection()
        except simplesqlite.NullDatabaseConnectionError:
            return

        self.connection.rollback()

    def commit(self):
        try:
            self.check_connection()
        except simplesqlite.NullDatabaseConnectionError:
            return

        self.connection.commit()

    def close(self):
        try:
            self.check_connection()
        except simplesqlite.NullDatabaseConnectionError:
            return

        self.commit()
        self.connection.close()
        self.__initialize_connection()

    @staticmethod
    def __validate_db_path(database_path):
        if database_path == simplesqlite.MEMORY_DB_NAME:
            return

        pathvalidate.validate_filename(os.path.basename(database_path))

    def __verify_sqlite_db_file(self, database_path):
        """
        :raises sqlite3.OperationalError: If unable to open database file
        """

        self.__validate_db_path(database_path)
        if not os.path.isfile(os.path.realpath(database_path)):
            raise IOError("file not found: " + database_path)

        connection = sqlite3.connect(database_path)
        connection.close()

    @staticmethod
    def __verify_value_matrix(field_list, value_matrix):
        """
        :param list/tuple field_list:
        :param list/tuple value_matrix: the list to test
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
            "  header: %d %s\n" % (len(field_list), str(field_list)) +
            "  # of miss match line: %d ouf of %d\n" % (
                len(miss_match_idx_list), len(value_matrix)) +
            "  e.g. value at line=%d, len=%d: %s\n" % (
                miss_match_idx_list[0],
                len(sample_miss_match_list), str(sample_miss_match_list))
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

    def validate_access_permission(self, valid_permission_list):
        """
        :param list/tuple valid_permission_list:
            List of permissions that access is allowed
        :raises ValueError: If database connection is invalid
        :raises IOError: If invalid permission

        .. seealso::

            :py:meth:`.check_connection`
        """

        self.check_connection()

        if dataproperty.is_empty_string(self.mode):
            raise ValueError("mode is not set")

        if self.mode not in valid_permission_list:
            raise IOError(str(valid_permission_list))

    def __get_column_valuetype(self, data_matrix):
        """
        Get value type for each column.

        :param list/tuple data_matrix:
        :return: { column_number : value_type }
        :rtype: dictionary
        """

        TYPENAME_TABLE = {
            dataproperty.Typecode.INT:    "INTEGER",
            dataproperty.Typecode.FLOAT:  "REAL",
            dataproperty.Typecode.STRING: "TEXT",
        }

        prop_extractor = dataproperty.PropertyExtractor()
        prop_extractor.data_matrix = data_matrix
        col_prop_list = prop_extractor.extract_column_property_list()

        return dict([
            [col, TYPENAME_TABLE[col_prop.typecode]]
            for col, col_prop in enumerate(col_prop_list)
        ])

    def __convert_none(self, value):
        if value is None:
            return "NULL"

        return value

    def __to_record(self, attr_name_list, value):
        try:
            # dictionary to list
            return [
                self.__convert_none(value.get(attr_name))
                for attr_name in attr_name_list
            ]
        except AttributeError:
            pass

        try:
            # namedtuple to list
            dict_value = value._asdict()
            return [
                self.__convert_none(dict_value.get(attr_name))
                for attr_name in attr_name_list
            ]
        except AttributeError:
            pass

        if dataproperty.is_list_or_tuple(value):
            return value

        raise ValueError("cannot convert to list")

    def __to_data_matrix(self, attr_name_list, data_matrix):
        return [
            self.__to_record(attr_name_list, record)
            for record in data_matrix
        ]

    def __create_table_config(self, table_config_matrix):
        if not self.__is_create_table_config:
            return

        attr_description_list = []
        for attr_name in self.TableConfiguration.ATTRIBUTE_NAME_LIST:
            if attr_name == "has_index":
                data_type = "INTEGER"
            else:
                data_type = "TEXT"

            attr_description_list.append("'%s' %s" % (attr_name, data_type))

        table_name = self.TableConfiguration.TABLE_NAME
        if not self.has_table(table_name):
            self.create_table(table_name, attr_description_list)

        self.insert_many(table_name, table_config_matrix)
