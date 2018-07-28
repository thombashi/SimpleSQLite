# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

import re

import dataproperty
import pathvalidate as pv
import typepy
from six.moves import range
from tabledata import (
    DataError,
    InvalidHeaderNameError,
    InvalidTableNameError,
    convert_idx_to_alphabet,
)
from tabledata.normalizer import AbstractTableDataNormalizer

from .converter import RecordConvertor
from .error import NameValidationError
from .query import Attr, AttrList


class SQLiteTableDataSanitizer(AbstractTableDataNormalizer):

    __RENAME_TEMPLATE = "rename_{:s}"

    def __init__(self, tabledata, dup_col_handler="error"):
        super(SQLiteTableDataSanitizer, self).__init__(tabledata)

        if typepy.is_null_string(tabledata.table_name):
            raise NameValidationError("table_name is empty")

        self.__upper_header_list = [
            header.upper() for header in self._tabledata.header_list if header
        ]
        self.__dup_col_handler = dup_col_handler

    def _preprocess_table_name(self):
        try:
            new_name = pv.sanitize_filename(self._tabledata.table_name, replacement_text="_")
            new_name = pv.replace_unprintable_char(new_name, replacement_text="")
            new_name = pv.replace_symbol(new_name, replacement_text="_")
            new_name = re.sub("_+", "_", new_name)
            new_name = new_name.replace(" ", "_")

            return new_name.strip("_")
        except TypeError:
            raise NameValidationError(
                "table name must be a string: actual='{}'".format(self._tabledata.table_name)
            )

    def _validate_table_name(self, table_name):
        try:
            pv.validate_sqlite_table_name(table_name)
        except pv.ValidReservedNameError:
            pass
        except (pv.InvalidReservedNameError, pv.InvalidCharError) as e:
            raise InvalidTableNameError(e)

    def _normalize_table_name(self, table_name):
        return self.__RENAME_TEMPLATE.format(table_name)

    def _preprocess_header(self, col_idx, header):
        if typepy.is_null_string(header):
            return self.__get_default_header(col_idx)

        if dataproperty.is_multibyte_str(header):
            return header

        return Attr.sanitize(header)

    def _validate_header_list(self):
        if typepy.is_empty_sequence(self._tabledata.header_list):
            raise ValueError("attribute name list is empty")

        for header in self._tabledata.header_list:
            self._validate_header(header)

    def _validate_header(self, header):
        try:
            pv.validate_sqlite_attr_name(header)
        except (pv.NullNameError, pv.ReservedNameError):
            pass
        except pv.InvalidCharError as e:
            raise InvalidHeaderNameError(e)

    def _normalize_header(self, header):
        return self.__RENAME_TEMPLATE.format(header)

    def _normalize_header_list(self):
        if typepy.is_empty_sequence(self._tabledata.header_list):
            try:
                return [
                    self.__get_default_header(col_idx)
                    for col_idx in range(len(self._tabledata.row_list[0]))
                ]
            except IndexError:
                raise DataError("header list and data body are empty")

        attr_name_list = AttrList.sanitize(
            super(SQLiteTableDataSanitizer, self)._normalize_header_list()
        )

        try:
            for attr_name in attr_name_list:
                pv.validate_sqlite_attr_name(attr_name)
        except pv.ReservedNameError:
            pass

        # duplicated attribute name handling ---
        from collections import Counter

        for key, count in Counter(attr_name_list).most_common():
            if count <= 1:
                continue

            if self.__dup_col_handler == "error":
                raise ValueError("duplicate column name: {}".format(key))

            # rename duplicate headers
            rename_target_idx_list = [i for i, attr in enumerate(attr_name_list) if attr == key][1:]
            suffix_count = 0
            for rename_target_idx in rename_target_idx_list:
                while True:
                    suffix_count += 1
                    attr_name_candidate = "{:s}_{:d}".format(key, suffix_count)
                    if attr_name_candidate in attr_name_list:
                        continue

                    attr_name_list[rename_target_idx] = attr_name_candidate
                    break

        return attr_name_list

    def _normalize_row_list(self, normalize_header_list):
        return RecordConvertor.to_record_list(normalize_header_list, self._tabledata.row_list)

    def __get_default_header(self, col_idx):
        i = 0
        while True:
            header = convert_idx_to_alphabet(col_idx + i)
            if header not in self.__upper_header_list:
                return header

            i += 1
