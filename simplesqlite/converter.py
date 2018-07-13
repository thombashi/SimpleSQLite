# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import absolute_import, unicode_literals

from decimal import Decimal


class RecordConvertor(object):
    @staticmethod
    def __to_sqlite_element(value):
        if isinstance(value, Decimal):
            return float(value)

        return value

    @classmethod
    def to_record(cls, attr_name_list, values):
        """
        Convert values to a record to be inserted into a database.

        :param list attr_name_list:
            List of attributes for the converting record.
        :param values: Values to be converted.
        :type values: |dict|/|namedtuple|/|list|/|tuple|
        :raises ValueError: If the ``values`` is invalid.
        """

        try:
            # from a namedtuple to a dict
            values = values._asdict()
        except AttributeError:
            pass

        try:
            # from a dictionary to a list
            return [cls.__to_sqlite_element(values.get(attr_name)) for attr_name in attr_name_list]
        except AttributeError:
            pass

        if isinstance(values, (tuple, list)):
            return [cls.__to_sqlite_element(value) for value in values]

        raise ValueError("cannot convert from {} to list".format(type(values)))

    @classmethod
    def to_record_list(cls, attr_name_list, value_matrix):
        """
        Convert a value matrix to records to be inserted into a database.

        :param list attr_name_list:
            List of attributes for the converting records.
        :param value_matrix: Values to be converted.
        :type value_matrix: list of |dict|/|namedtuple|/|list|/|tuple|

        .. seealso:: :py:meth:`.to_record`
        """

        return [cls.to_record(attr_name_list, record) for record in value_matrix]
