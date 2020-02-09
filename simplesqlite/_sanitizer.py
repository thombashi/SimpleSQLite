# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

import re
from collections import Counter

import dataproperty
import pathvalidate as pv
import six
import typepy
from pathvalidate import (
    InvalidCharError,
    InvalidReservedNameError,
    NullNameError,
    ReservedNameError,
    ValidReservedNameError,
)
from six.moves import range
from tabledata import (
    DataError,
    InvalidHeaderNameError,
    InvalidTableNameError,
    convert_idx_to_alphabet,
)
from tabledata.normalizer import AbstractTableDataNormalizer

from ._validator import validate_sqlite_attr_name, validate_sqlite_table_name
from .converter import RecordConvertor
from .error import NameValidationError
from .query import Attr, AttrList


class SQLiteTableDataSanitizer(AbstractTableDataNormalizer):

    __RENAME_TEMPLATE = "rename_{:s}"

    @property
    def _type_hints(self):
        if self.__is_type_inference:
            return self._tabledata.dp_extractor.column_type_hints

        if self._tabledata.dp_extractor.column_type_hints:
            return [typepy.String for _ in self._tabledata.dp_extractor.column_type_hints]

        if self.__upper_headers:
            return [typepy.String for _ in self.__upper_headers]

        return None

    def __init__(
        self, tabledata, dup_col_handler="error", is_type_inference=True, max_workers=None
    ):
        tabledata.max_workers = max_workers

        super(SQLiteTableDataSanitizer, self).__init__(tabledata)

        if typepy.is_null_string(tabledata.table_name):
            raise NameValidationError("table_name is empty")

        self.__upper_headers = []
        for header in self._tabledata.headers:
            if not header:
                continue
            try:
                header = header.upper()
            except AttributeError:
                header = six.text_type(header).upper()

            self.__upper_headers.append(header)

        self.__dup_col_handler = dup_col_handler
        self.__is_type_inference = is_type_inference

    def _preprocess_table_name(self):
        try:
            new_name = pv.sanitize_filename(self._tabledata.table_name, replacement_text="_")
        except TypeError:
            raise NameValidationError(
                "table name must be a string: actual='{}'".format(self._tabledata.table_name)
            )

        new_name = pv.replace_unprintable_char(new_name, replacement_text="")
        new_name = pv.replace_symbol(new_name, replacement_text="_")
        new_name = new_name.replace(" ", "_")
        new_name = re.sub("_+", "_", new_name)
        new_name = new_name.strip("_")

        return new_name

    def _validate_table_name(self, table_name):
        try:
            validate_sqlite_table_name(table_name)
        except ValidReservedNameError:
            pass
        except (InvalidReservedNameError, InvalidCharError) as e:
            raise InvalidTableNameError(e)

    def _normalize_table_name(self, table_name):
        return self.__RENAME_TEMPLATE.format(table_name)

    def _preprocess_header(self, col_idx, header):
        if typepy.is_null_string(header):
            return self.__get_default_header(col_idx)

        if dataproperty.is_multibyte_str(header):
            return header

        return Attr.sanitize(header)

    def _validate_headers(self):
        if typepy.is_empty_sequence(self._tabledata.headers):
            raise ValueError("attribute name list is empty")

        for header in self._tabledata.headers:
            self._validate_header(header)

    def _validate_header(self, header):
        try:
            validate_sqlite_attr_name(header)
        except (NullNameError, ReservedNameError):
            pass
        except InvalidCharError as e:
            raise InvalidHeaderNameError(e)

    def _normalize_header(self, header):
        return self.__RENAME_TEMPLATE.format(header)

    def _normalize_headers(self):
        if typepy.is_empty_sequence(self._tabledata.headers):
            try:
                return [
                    self.__get_default_header(col_idx)
                    for col_idx in range(len(self._tabledata.rows[0]))
                ]
            except IndexError:
                raise DataError("header list and data body are empty")

        attr_name_list = AttrList.sanitize(
            super(SQLiteTableDataSanitizer, self)._normalize_headers()
        )

        try:
            for attr_name in attr_name_list:
                validate_sqlite_attr_name(attr_name)
        except ReservedNameError:
            pass

        # duplicated attribute name handling ---
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

    def _normalize_rows(self, normalize_headers):
        return RecordConvertor.to_records(normalize_headers, self._tabledata.rows)

    def __get_default_header(self, col_idx):
        i = 0
        while True:
            header = convert_idx_to_alphabet(col_idx + i)
            if header not in self.__upper_headers:
                return header

            i += 1
