# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


from __future__ import absolute_import

import dataproperty

from ..error import InvalidDataError
from ..data import TableData
from .core import SpreadSheetLoader


class GoogleSheetsTableLoader(SpreadSheetLoader):
    """
    Concrete class of Google Spreadsheet loader.

    Requirements:

        - `gspread <https://github.com/burnash/gspread>`_
        - `oauth2client <https://pypi.python.org/pypi/oauth2client>`_
        - `pyOpenSSL <https://pypi.python.org/pypi/pyOpenSSL>`_
    """

    @property
    def _sheet_name(self):
        return self._worksheet.title

    @property
    def _row_count(self):
        return self._worksheet.row_count

    @property
    def _col_count(self):
        return self._worksheet.col_count

    def __init__(self, file_path=None):
        super(GoogleSheetsTableLoader, self).__init__(file_path)

        self.title = None
        self.start_row = 0

        self.__all_values = None

    def make_table_name(self):
        """
        |make_table_name|

            ================  ===========================
            format specifier  value after the replacement
            ================  ===========================
            ``%(filename)s``  Filename of the workbook
            ``%(title)s``     Name of the spreadsheet
            ================  ===========================

        :return: Table name.
        :rtype: str
        """

        self._validate_title()
        table_name = super(
            GoogleSheetsTableLoader, self).make_table_name()

        return table_name.replace("%(title)s", self.title)

    def load(self):
        """
        Load table data from a Google Spreadsheet.
        |load_desc|

        :return:
            |load_return|
            :py:meth:`~.GoogleSheetsTableLoader.make_table_name`.
        :rtype: iterator of |TableData|
        :raises InvalidDataError: If the header row is not found.
        """

        import gspread
        from oauth2client.service_account import ServiceAccountCredentials

        self._validate_table_name()
        self._validate_title()

        scope = ['https://spreadsheets.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.source, scope)

        gc = gspread.authorize(credentials)
        for worksheet in gc.open(self.title).worksheets():
            self._worksheet = worksheet
            self.__all_values = worksheet.get_all_values()

            if self._is_empty_sheet():
                continue

            self.__strip_empty_col()

            value_matrix = self.__all_values[self._get_start_row_idx():]
            header_list = value_matrix[0]
            record_list = value_matrix[1:]

            yield TableData(self.make_table_name(), header_list, record_list)

    def _is_empty_sheet(self):
        return len(self.__all_values) <= 1

    def _get_start_row_idx(self):
        row_idx = 0
        for row_value_list in self.__all_values:
            if all([
                dataproperty.is_not_empty_string(value)
                for value in row_value_list
            ]):
                break

            row_idx += 1

        return self.start_row + row_idx

    def _validate_title(self):
        if dataproperty.is_empty_string(self.title):
            raise ValueError("spreadsheet title is empty")

    def __strip_empty_col(self):
        col_idx = 0
        t_value_matrix = zip(*self.__all_values)

        for col_value_list in t_value_matrix:
            if any([
                dataproperty.is_not_empty_string(value)
                for value in col_value_list
            ]):
                break

            col_idx += 1

        self.__all_values = zip(*t_value_matrix[col_idx:])
