"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from collections import Counter
from typing import List, Optional, Sequence, cast

import dataproperty
import pathvalidate as pv
import typepy
from dataproperty.typing import TypeHint
from pathvalidate.error import ErrorReason, ValidationError
from tabledata import (
    DataError,
    InvalidHeaderNameError,
    InvalidTableNameError,
    TableData,
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
    def _type_hints(self) -> Optional[List[TypeHint]]:
        if self.__is_type_inference:
            return self._tabledata.dp_extractor.column_type_hints

        if self._tabledata.dp_extractor.column_type_hints:
            return [typepy.String for _ in self._tabledata.dp_extractor.column_type_hints]

        if self.__upper_headers:
            return [typepy.String for _ in self.__upper_headers]

        return None

    def __init__(
        self,
        table_data: TableData,
        dup_col_handler: str = "error",
        is_type_inference: bool = True,
        max_workers: Optional[int] = None,
    ) -> None:
        table_data.max_workers = max_workers  # type: ignore

        super().__init__(table_data)

        if typepy.is_null_string(table_data.table_name):
            raise NameValidationError("table_name is empty")

        self.__upper_headers = []
        for header in self._tabledata.headers:
            if not header:
                continue
            try:
                header = header.upper()
            except AttributeError:
                header = str(header).upper()

            self.__upper_headers.append(header)

        self.__dup_col_handler = dup_col_handler
        self.__is_type_inference = is_type_inference

    def _preprocess_table_name(self) -> str:
        try:
            new_name = str(pv.sanitize_filename(self._tabledata.table_name, replacement_text="_"))
        except TypeError:
            raise NameValidationError(
                f"table name must be a string: actual='{self._tabledata.table_name}'"
            )

        new_name = pv.replace_unprintable_char(new_name, replacement_text="")
        new_name = pv.replace_symbol(
            new_name,
            replacement_text="_",
            is_replace_consecutive_chars=True,
            is_strip=True,
        )

        return new_name

    def _validate_table_name(self, table_name: str) -> None:
        try:
            validate_sqlite_table_name(table_name)
        except ValidationError as e:
            if (
                e.reason == ErrorReason.RESERVED_NAME and e.reusable_name is False
            ) or e.reason == ErrorReason.INVALID_CHARACTER:
                raise InvalidTableNameError(e)
            elif e.reason == ErrorReason.RESERVED_NAME:
                pass
            else:
                raise

    def _normalize_table_name(self, table_name: str) -> str:
        return self.__RENAME_TEMPLATE.format(table_name)

    def _preprocess_header(self, col_idx: int, header: Optional[str]) -> str:
        if typepy.is_null_string(header):
            return self.__get_default_header(col_idx)

        if dataproperty.is_multibyte_str(header):
            return cast(str, header)

        return Attr.sanitize(cast(str, header))

    def _validate_headers(self) -> None:
        if typepy.is_empty_sequence(self._tabledata.headers):
            raise ValueError("attribute name list is empty")

        for header in self._tabledata.headers:
            self._validate_header(header)

    def _validate_header(self, header: str) -> None:
        try:
            validate_sqlite_attr_name(header)
        except ValidationError as e:
            if e.reason in (ErrorReason.NULL_NAME, ErrorReason.RESERVED_NAME):
                pass
            elif e.reason == ErrorReason.INVALID_CHARACTER:
                raise InvalidHeaderNameError(e)
            else:
                raise

    def _normalize_header(self, header: str) -> str:
        return self.__RENAME_TEMPLATE.format(header)

    def _normalize_headers(self) -> List[str]:
        if typepy.is_empty_sequence(self._tabledata.headers):
            try:
                return [
                    self.__get_default_header(col_idx)
                    for col_idx in range(len(self._tabledata.rows[0]))
                ]
            except IndexError:
                raise DataError("header list and data body are empty")

        attr_name_list = AttrList.sanitize(super()._normalize_headers())  # type: ignore

        try:
            for attr_name in attr_name_list:
                validate_sqlite_attr_name(attr_name)
        except ValidationError as e:
            if e.reason == ErrorReason.RESERVED_NAME:
                pass
            else:
                raise

        # duplicated attribute name handling ---
        for key, count in Counter(attr_name_list).most_common():
            if count <= 1:
                continue

            if self.__dup_col_handler == "error":
                raise ValueError(f"duplicate column name: {key}")

            # rename duplicate headers
            rename_target_idx_list = [i for i, attr in enumerate(attr_name_list) if attr == key][1:]
            suffix_count = 0
            for rename_target_idx in rename_target_idx_list:
                while True:
                    suffix_count += 1
                    attr_name_candidate = f"{key:s}_{suffix_count:d}"
                    if attr_name_candidate in attr_name_list:
                        continue

                    attr_name_list[rename_target_idx] = attr_name_candidate
                    break

        return attr_name_list

    def _normalize_rows(self, normalize_headers: Sequence[str]) -> List:
        return RecordConvertor.to_records(normalize_headers, self._tabledata.rows)

    def __get_default_header(self, col_idx: int) -> str:
        i = 0
        while True:
            header = convert_idx_to_alphabet(col_idx + i)
            if header not in self.__upper_headers:
                return header

            i += 1
