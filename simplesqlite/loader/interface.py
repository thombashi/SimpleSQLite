# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


from __future__ import absolute_import
import abc

import dataproperty
import six


@six.add_metaclass(abc.ABCMeta)
class TableLoaderInterface(object):
    """
    Interface class of table loader class.
    """

    @abc.abstractmethod
    def load(self):   # pragma: no cover
        pass

    @abc.abstractmethod
    def _validate(self):   # pragma: no cover
        pass

    @abc.abstractmethod
    def _validate_table_name(self):   # pragma: no cover
        pass

    @abc.abstractmethod
    def _validate_source(self):   # pragma: no cover
        pass


class TableLoader(TableLoaderInterface):
    """
    Abstract class of table data file loader.
    """

    def __init__(self, source):
        self.table_name = None
        self.source = source

    def make_table_name(self):
        self._validate()

        return self.table_name

    def _validate(self):
        self._validate_table_name()
        self._validate_source()

    def _validate_table_name(self):
        try:
            if dataproperty.is_empty_string(self.table_name):
                raise ValueError("invalid table name")
        except (TypeError, AttributeError):
            raise TypeError("table_name expected to a string")

    def _validate_source(self):
        if dataproperty.is_empty_string(self.source):
            raise ValueError("data source is empty")
