# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import
import abc
import threading

import dataproperty
import path
import six

from .._error import InvalidTableNameError
from .._func import validate_table_name
from .constant import TableNameTemplate as tnt
from .error import InvalidDataError


@six.add_metaclass(abc.ABCMeta)
class TableLoaderInterface(object):
    """
    Interface class of table loader class.
    """

    @abc.abstractmethod
    def load(self):  # pragma: no cover
        pass

    @abc.abstractproperty
    def format_name(self):  # pragma: no cover
        pass

    @abc.abstractmethod
    def make_table_name(self):  # pragma: no cover
        pass

    @abc.abstractmethod
    def inc_table_count(self):  # pragma: no cover
        pass


class TableLoader(TableLoaderInterface):
    """
    Abstract class of table data file loader.

    .. py:attribute:: table_name

        Table name string.

    .. py:attribute:: source

        Table data source to load.
    """

    def __init__(self, source):
        self.table_name = tnt.DEFAULT
        self.source = source

        self.__table_count_lock = threading.Lock()
        self.__global_table_count = 0
        self.__format_table_count = {}

    def get_format_table_count(self):
        return self.__format_table_count.get(self.format_name, 0)

    def make_table_name(self):
        """
        |make_table_name|

            ===================  ========================================
            format specifier     value after the replacement
            ===================  ========================================
            ``%(format_name)s``  return value of ``format_name`` property
            ``%(format_id)s``    unique number in the same format
            ``%(global_id)s``    unique number in all of the format
            ===================  ========================================

        :return: Table name.
        :rtype: str
        """

        return self._sanitize_table_name(self.__make_table_name())

    def inc_table_count(self):
        with self.__table_count_lock:
            self.__global_table_count += 1
            self.__format_table_count[self.format_name] = (
                self.get_format_table_count() + 1)

    @abc.abstractmethod
    def _get_default_table_name_template(self):  # pragma: no cover
        pass

    def _validate(self):
        self._validate_table_name()
        self._validate_source()

    def _validate_table_name(self):
        try:
            if dataproperty.is_empty_string(self.table_name):
                raise ValueError("table name is empty")
        except (TypeError, AttributeError):
            raise TypeError("table_name expected to a string")

    def _validate_source(self):
        if dataproperty.is_empty_string(self.source):
            raise InvalidDataError("data source is empty")

    def _make_file_table_name(self):
        filename = ""
        if dataproperty.is_not_empty_string(self.source):
            filename = path.Path(self.source).namebase

        table_name = self.__make_table_name().replace(
            tnt.FILENAME, filename)

        if dataproperty.is_empty_string(table_name):
            raise ValueError(
                "table name is empty after the template replacement")

        return self._sanitize_table_name(table_name)

    def __make_table_name(self):
        self._validate_table_name()

        table_name = self.table_name.replace(
            tnt.DEFAULT, self._get_default_table_name_template())
        table_name = table_name.replace(
            tnt.FORMAT_NAME, self.format_name)
        table_name = table_name.replace(
            tnt.FORMAT_ID,
            str(self.__format_table_count.get(self.format_name, 0)))
        table_name = table_name.replace(
            tnt.GLOBAL_ID, str(self.__global_table_count))

        return table_name

    def _sanitize_table_name(self, table_name):
        try:
            validate_table_name(table_name)
            return table_name
        except InvalidTableNameError:
            return "{:s}_{:s}".format(table_name, self.format_name)
