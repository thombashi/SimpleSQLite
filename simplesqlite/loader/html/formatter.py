# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import

import bs4
import dataproperty

from ..constant import TableNameTemplate as tnt
from ..data import TableData
from ..error import InvalidDataError
from ..formatter import TableFormatter


class HtmlTableFormatter(TableFormatter):

    def __init__(self, source_data):
        super(HtmlTableFormatter, self).__init__(source_data)

        try:
            self.__soup = bs4.BeautifulSoup(self._source_data, "lxml")
        except bs4.FeatureNotFound:
            self.__soup = bs4.BeautifulSoup(self._source_data, "html.parser")

    def to_table_data(self):
        self._validate_source_data()

        for table in self.__soup.find_all("table"):
            tabledata = self.__parse_html(table)

            yield tabledata

    def _make_table_name(self):
        table_name = self._loader.make_table_name()
        key = self.__table_id
        if dataproperty.is_empty_string(key):
            key = "{:s}{:d}".format(
                self._loader.format_name,
                self._loader.get_format_table_count())

        return table_name.replace(tnt.KEY, key)

    def __parse_html(self, table):
        header_list = []
        data_matrix = []

        self.__table_id = table.get("id")

        row_list = table.find_all("tr")
        for row in row_list:
            col_list = row.find_all("td")
            if dataproperty.is_empty_sequence(col_list):
                th_list = row.find_all("th")
                if dataproperty.is_empty_sequence(th_list):
                    continue

                header_list = [row.text.strip() for row in th_list]
                continue

            data_list = [value.text.strip() for value in col_list]
            data_matrix.append(data_list)

        self._loader.inc_table_count()

        return TableData(
            self._make_table_name(), header_list, data_matrix)
