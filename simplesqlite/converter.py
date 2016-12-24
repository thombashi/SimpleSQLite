# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import
from decimal import Decimal

import dataproperty


class RecordConvertor(object):

    @staticmethod
    def __to_sqlite_element(value):
        if isinstance(value, Decimal):
            return float(value)

        if value is None:
            return "NULL"

        return value

    @classmethod
    def to_record(cls, attr_name_list, values):
        """
        Convert values to a record to be inserted into a database.

        :param list attr_name_list:
            List of the attributes for the converting record.
        :param values: Value to be converted.
        :type values: |dict|/|namedtuple|/|list|/|tuple|
        :raises ValueError: If the ``values`` is invalid.
        """

        try:
            # dictionary to list
            return [
                cls.__to_sqlite_element(values.get(attr_name))
                for attr_name in attr_name_list
            ]
        except AttributeError:
            pass

        try:
            # namedtuple to list
            dict_value = values._asdict()
            return [
                cls.__to_sqlite_element(dict_value.get(attr_name))
                for attr_name in attr_name_list
            ]
        except AttributeError:
            pass

        if dataproperty.is_list_or_tuple(values):
            return [
                cls.__to_sqlite_element(value) for value in values
            ]

        raise ValueError("cannot convert to list")

    @classmethod
    def to_record_list(cls, attr_name_list, data_matrix):
        """
        Convert matrix to records to be inserted into a database.

        :param list attr_name_list:
            List of the attributes for the converting record.
        :param data_matrix: Value to be converted.
        :type data_matrix: list of |dict|/|namedtuple|/|list|/|tuple|

        .. seealso:: :py:meth:`.to_record`
        """

        return [
            cls.to_record(attr_name_list, record)
            for record in data_matrix
        ]
