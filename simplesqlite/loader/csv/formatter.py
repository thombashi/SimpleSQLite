# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


from __future__ import absolute_import

import dataproperty

from ..data import TableData
from ..error import InvalidDataError
from ..formatter import TableFormatter


class CsvTableFormatter(TableFormatter):

    def to_table_data(self):
        self._validate_source_data()

        if dataproperty.is_empty_list_or_tuple(self._loader.header_list):
            header_list = self._source_data[0]

            if any([
                dataproperty.is_empty_string(header) for header in header_list
            ]):
                raise InvalidDataError(
                    "the first line includes empty string item: "
                    "the first line expected to contain header data.")

            data_matrix = self._source_data[1:]
        else:
            header_list = self._loader.header_list
            data_matrix = self._source_data

        if len(data_matrix) == 0:
            raise InvalidDataError(
                "data row must be greater or equal than one")

        yield TableData(
            self._loader.make_table_name(),
            header_list, data_matrix)

    def _validate_source_data(self):
        if len(self._source_data) == 0:
            raise InvalidDataError("csv data is empty")
