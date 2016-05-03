# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


from __future__ import absolute_import
import abc

from ..interface import TableLoader


class SpreadSheetLoader(TableLoader):
    """
    Abstract class of table data.
    Especially spreadsheets that consists multiple rows.

    .. py:attribute:: table_name

        Table name string. Defaults to ``%(sheet)s``.

    .. py:attribute:: start_row

        The first row to search header row.
    """

    def __init__(self, source):
        super(SpreadSheetLoader, self).__init__(source)

        self.table_name = "%(sheet)s"
        self.start_row = 0
        self._worksheet = None
        self._start_col_idx = None
        self._end_col_idx = None

    @abc.abstractproperty
    def _sheet_name(self):   # pragma: no cover
        pass

    @abc.abstractproperty
    def _row_count(self):   # pragma: no cover
        pass

    @abc.abstractproperty
    def _col_count(self):   # pragma: no cover
        pass

    @abc.abstractmethod
    def _is_empty_sheet(self):   # pragma: no cover
        pass

    @abc.abstractmethod
    def _get_start_row_idx(self):   # pragma: no cover
        pass

    def make_table_name(self):
        self._validate()

        table_name = super(SpreadSheetLoader, self).make_table_name()

        return table_name.replace("%(sheet)s", self._sheet_name)
