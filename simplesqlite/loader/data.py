# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import

import pathvalidate

from .._func import validate_table_name


class TableData(object):
    """
    Class to represent a table data structure.

    .. py:attribute:: table_name

        Name of the table.

    .. py:attribute:: header_list

        List of header names.

    .. py:attribute:: record_list

        List of records of the table.
    """

    @property
    def table_name(self):
        return self.__table_name

    @property
    def header_list(self):
        return self.__header_list

    @property
    def record_list(self):
        return self.__record_list

    def __init__(self, table_name, header_list, record_list):
        validate_table_name(table_name)

        self.__table_name = table_name
        self.__header_list = header_list
        self.__record_list = record_list

        self.__sanitize_header_list()

    def __repr__(self):
        return "table_name={}, header_list={} record_list={}".format(
            self.table_name, self.header_list, self.record_list)

    def __eq__(self, other):
        return all([
            self.table_name == other.table_name,
            self.header_list == other.header_list,
            self.record_list == other.record_list,
        ])

    def __sanitize_header_list(self):
        new_header_list = []

        for i, header in enumerate(self.header_list):
            try:
                pathvalidate.validate_sqlite_attr_name(header)
                new_header = header
            except pathvalidate.ReservedNameError as e:
                rename_count = 0
                while True:
                    new_header = "{:s}_rename{:d}".format(header, rename_count)
                    if all([
                        new_header not in self.header_list[i:],
                        new_header not in new_header_list,
                    ]):
                        break

                    rename_count += 1

            new_header_list.append(new_header)

        self.__header_list = new_header_list
