"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, List, Sequence, Union

from ._logger import logger


def default_datetime_converter(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S%z")


class RecordConvertor:
    @staticmethod
    def __to_sqlite_element(
        value: Any, attr: Union[int, str], datetime_converter: Callable[[datetime], str]
    ) -> Any:
        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, int):
            # INTEGER. The value is a signed integer,
            # stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
            # https://www.sqlite.org/datatype3.html
            if not (-9223372036854775808 < value < 9223372036854775807):
                raise OverflowError(attr)

        if isinstance(value, datetime):
            # TODO: add an interface to specify datetime_converter
            return datetime_converter(value)

        return value

    @classmethod
    def to_record(cls, attr_names: Sequence[str], values) -> List:
        """
        Convert values to a record to be inserted into a database.

        :param list attr_names:
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

        datetime_converter = default_datetime_converter

        try:
            # from a dictionary to a list
            return [
                cls.__to_sqlite_element(values.get(attr_name), attr_name, datetime_converter)
                for attr_name in attr_names
            ]
        except AttributeError:
            pass

        if isinstance(values, (tuple, list)):
            return [
                cls.__to_sqlite_element(value, col, datetime_converter)
                for col, value in enumerate(values)
            ]

        raise ValueError("cannot convert from {} to list".format(type(values)))

    @classmethod
    def to_records(cls, attr_names: Sequence[str], value_matrix: Sequence) -> List:
        """
        Convert a value matrix to records to be inserted into a database.

        :param list attr_names:
            List of attributes for the converting records.
        :param value_matrix: Values to be converted.
        :type value_matrix: list of |dict|/|namedtuple|/|list|/|tuple|

        .. seealso:: :py:meth:`.to_record`
        """

        records = []
        error_msgs = []

        for row_idx, record in enumerate(value_matrix):
            try:
                records.append(cls.to_record(attr_names, record))
            except OverflowError as e:
                try:
                    if isinstance(e.args[0], int):
                        col_idx = e.args[0]
                        col = "{} ({})".format(attr_names[col_idx], col_idx)
                    else:
                        col = e.args[0]
                except IndexError as e:
                    logger.error(e)
                    continue

                error_msgs.append("  overflow int found: row={}, col={}".format(row_idx, col))

        if error_msgs:
            raise OverflowError("failed to convert:\n" + "\n".join(error_msgs))

        return records
