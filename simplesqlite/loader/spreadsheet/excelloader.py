# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


from __future__ import absolute_import

import path
from six.moves import range
import xlrd

from ..error import InvalidDataError
from ..data import TableData
from .core import SpreadSheetLoader


class ExcelTableFileLoader(SpreadSheetLoader):
    """
    Concrete class of Microsoft Excel |TM| file loader.
    """

    @property
    def _sheet_name(self):
        return self._worksheet.name

    @property
    def _row_count(self):
        return self._worksheet.nrows

    @property
    def _col_count(self):
        return self._worksheet.ncols

    def __init__(self, file_path=None):
        super(ExcelTableFileLoader, self).__init__(file_path)

    def make_table_name(self):
        """
        |make_table_name|

            ================  ===========================
            format specifier  value after the replacement
            ================  ===========================
            ``%(filename)s``  Filename of the workbook
            ``%(sheet)s``     Name of the sheet
            ================  ===========================

        :return: Table name.
        :rtype: str
        """

        self._validate_source()
        table_name = super(ExcelTableFileLoader, self).make_table_name()

        return table_name.replace(
            "%(filename)s", path.Path(self.source).namebase)

    def load(self):
        """
        Load table data from a Excel file.
        |load_desc|

        :return:
            |load_return|
            :py:meth:`~.ExcelTableFileLoader.make_table_name`.
        :rtype: iterator of |TableData|
        :raises InvalidDataError: If the header row is not found.
        """

        self._validate()

        try:
            workbook = xlrd.open_workbook(self.source)
        except xlrd.biffh.XLRDError as e:
            raise InvalidDataError(e)

        for worksheet in workbook.sheets():
            self._worksheet = worksheet

            if self._is_empty_sheet():
                continue

            self.__extract_not_empty_col_idx()

            try:
                start_row_idx = self._get_start_row_idx()
            except InvalidDataError:
                continue

            header_list = self.__get_row_values(start_row_idx)
            record_list = [
                self.__get_row_values(row_idx)
                for row_idx in range(start_row_idx + 1, self._row_count)
            ]

            yield TableData(self.make_table_name(), header_list, record_list)

    def _is_empty_sheet(self):
        return any([
            self._col_count == 0,
            self._row_count <= 1,
            # nrows == 1 means exists header row only
        ])

    def _get_start_row_idx(self):
        for row_idx in range(self.start_row, self._row_count):
            if self.__is_header_row(row_idx):
                break
        else:
            raise InvalidDataError("header row is not found")

        return row_idx

    def __is_header_row(self, row_idx):
        cell_type_list = self._worksheet.row_types(
            row_idx, self._start_col_idx, self._end_col_idx + 1)
        return xlrd.XL_CELL_EMPTY not in cell_type_list

    @staticmethod
    def __is_empty_cell_type_list(cell_type_list):
        return all([
            cell_type == xlrd.XL_CELL_EMPTY
            for cell_type in cell_type_list
        ])

    def __extract_not_empty_col_idx(self):
        col_idx_list = [
            col_idx
            for col_idx in range(self._col_count)
            if not self.__is_empty_cell_type_list(
                self._worksheet.col_types(col_idx))
        ]

        self._start_col_idx = min(col_idx_list)
        self._end_col_idx = max(col_idx_list)

    def __get_row_values(self, row_idx):
        return self._worksheet.row_values(
            row_idx, self._start_col_idx, self._end_col_idx + 1)
