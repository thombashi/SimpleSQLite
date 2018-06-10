# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

import re

import dataproperty
import pathvalidate as pv
import typepy
from tabledata.normalizer import AbstractTableDataNormalizer

from six.moves import range

from .error import NameValidationError
from .query import Attr


class SQLiteTableDataSanitizer(AbstractTableDataSanitizer):

    __RE_PREPROCESS = re.compile("[^a-zA-Z0-9_]+")
    __RENAME_TEMPLATE = "rename_{:s}"

    def __init__(self, tabledata):
        super(SQLiteTableDataSanitizer, self).__init__(tabledata)

        self.__upper_header_list = [
            header.upper() for header in self._tabledata.header_list if header
        ]

    def _preprocess_table_name(self):
        try:
            new_name = self.__RE_PREPROCESS.sub("_", self._tabledata.table_name)
            return new_name.strip("_")
        except TypeError:
            raise NameValidationError("table name must be a string: actual='{}'".format(
                self._tabledata.table_name))

    def _validate_table_name(self, table_name):
        try:
            pv.validate_sqlite_table_name(table_name)
        except pv.ValidReservedNameError:
            pass
        except (pv.InvalidReservedNameError, pv.InvalidCharError, pv.NullNameError) as e:
            raise NameValidationError(e)

    def _sanitize_table_name(self, table_name):
        return self.__RENAME_TEMPLATE.format(table_name)

    def _preprocess_header(self, col_idx, header):
        if typepy.is_null_string(header):
            return self.__get_default_header(col_idx)

        if dataproperty.is_multibyte_str(header):
            return header

        return Attr.sanitize(header)

    def _validate_header(self, header):
        try:
            pv.validate_sqlite_attr_name(header)
        except (pv.NullNameError, pv.ReservedNameError):
            pass
        except pv.InvalidCharError as e:
            raise InvalidHeaderNameError(e)

    def _sanitize_header(self, header):
        return self.__RENAME_TEMPLATE.format(header)

    def _sanitize_header_list(self):
        if typepy.is_empty_sequence(self._tabledata.header_list):
            try:
                return [
                    self.__get_default_header(col_idx)
                    for col_idx in range(len(self._tabledata.value_dp_matrix[0]))
                ]
            except IndexError:
                raise DataError("header list and data body are empty")

        return super(SQLiteTableDataSanitizer, self)._sanitize_header_list()

    def __get_default_header(self, col_idx):
        i = 0
        while True:
            header = convert_idx_to_alphabet(col_idx + i)
            if header not in self.__upper_header_list:
                return header

            i += 1
