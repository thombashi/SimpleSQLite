# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


import dataproperty


class RecordConvertor(object):

    @staticmethod
    def __convert_none(value):
        if value is None:
            return "NULL"

        return value

    @classmethod
    def to_record(cls, attr_name_list, value):
        """
        Convert values to a record to be inserted into a database.

        :param list attr_name_list:
            List of the attributes for the converting record.
        :param value: Value to be converted.
        :type value: |dict|/|namedtuple|/|list|/|tuple|
        :raises ValueError: If the ``value`` is invalid.
        """

        try:
            # dictionary to list
            return [
                cls.__convert_none(value.get(attr_name))
                for attr_name in attr_name_list
            ]
        except AttributeError:
            pass

        try:
            # namedtuple to list
            dict_value = value._asdict()
            return [
                cls.__convert_none(dict_value.get(attr_name))
                for attr_name in attr_name_list
            ]
        except AttributeError:
            pass

        if dataproperty.is_list_or_tuple(value):
            return list(value)

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
