# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


from __future__ import absolute_import
import collections


class TableData(collections.namedtuple(
        "TableData", "table_name header_list record_list")):
    """
    |namedtuple| to represent the table data structure.

    .. py:attribute:: table_name

        Name of the table.

    .. py:attribute:: header_list

        List of header names.

    .. py:attribute:: record_list

        List of records of the table.
    """
