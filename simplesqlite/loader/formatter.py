# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


from __future__ import absolute_import

import abc
import six

from .acceptor import LoaderAcceptor
from .error import InvalidDataError


@six.add_metaclass(abc.ABCMeta)
class TableFormatterInterface(object):
    """
    Abstract class of table data validator.
    """

    @abc.abstractmethod
    def to_table_data(self):   # pragma: no cover
        pass

    @abc.abstractmethod
    def _validate_source_data(self):   # pragma: no cover
        pass


class TableFormatter(LoaderAcceptor, TableFormatterInterface):
    """
    Abstract class of |TableData| formatter.
    """

    def _validate_source_data(self):
        if len(self._source_data) == 0:
            raise InvalidDataError("souce data is empty")

    def __init__(self, source_data):
        self._source_data = source_data
