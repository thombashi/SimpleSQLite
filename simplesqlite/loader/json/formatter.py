# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import
import abc

import jsonschema
import six

from ..constant import TableNameTemplate as tnt
from ..acceptor import LoaderAcceptor
from ..data import TableData
from ..error import ValidationError
from ..formatter import TableFormatter
from ..formatter import TableFormatterInterface


class JsonConverter(TableFormatterInterface, LoaderAcceptor):
    """
    Abstract class of JSON data converter.
    """

    def __init__(self, json_buffer):
        self._buffer = json_buffer

    @abc.abstractproperty
    def _schema(self):  # pragma: no cover
        pass

    def _validate_source_data(self):
        """
        :raises ValidationError:
        """

        try:
            jsonschema.validate(self._buffer, self._schema)
        except jsonschema.ValidationError as e:
            raise ValidationError(e)


class SingleJsonTableConverter(JsonConverter):
    """
    Concrete class of JSON table data converter.
    """

    def __init__(self, json_buffer):
        super(SingleJsonTableConverter, self).__init__(json_buffer)

    @property
    def _schema(self):
        return {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": {
                    "anyOf": [
                        {"type": "string"},
                        {"type": "number"},
                        {"type": "null"},
                    ],
                },
            },
        }

    def to_table_data(self):
        """
        :raises ValueError:
        :raises simplesqlite.loader.ValidationError:
        """

        self._validate_source_data()

        attr_name_set = set()
        for json_record in self._buffer:
            attr_name_set = attr_name_set.union(list(json_record.keys()))

        self._loader.inc_table_count()

        yield TableData(
            self._make_table_name(), sorted(attr_name_set), self._buffer)

    def _make_table_name(self):
        table_name = self._loader.make_table_name()
        table_name = table_name.replace(
            tnt.KEY, "{:s}{:s}".format(tnt.FORMAT_NAME, tnt.FORMAT_ID))
        table_name = table_name.replace(
            tnt.FORMAT_NAME, self._loader.format_name)
        table_name = table_name.replace(
            tnt.FORMAT_ID, str(self._loader.get_format_table_count()))

        return table_name


class MultipleJsonTableConverter(JsonConverter):
    """
    Concrete class of JSON table data converter.
    """

    @property
    def _schema(self):
        return {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": {
                        "anyOf": [
                            {"type": "string"},
                            {"type": "number"},
                            {"type": "null"},
                        ],
                    },
                },
            },
        }

    def __init__(self, json_buffer):
        super(MultipleJsonTableConverter, self).__init__(json_buffer)

        self.__table_key = None

    def to_table_data(self):
        """
        :raises ValueError:
        :raises simplesqlite.loader.ValidationError:
        """

        self._validate_source_data()

        for table_key, json_record_list in six.iteritems(self._buffer):
            attr_name_set = set()
            for json_record in json_record_list:
                attr_name_set = attr_name_set.union(list(json_record.keys()))

            self._loader.inc_table_count()

            self.__table_key = table_key

            yield TableData(
                self._make_table_name(),
                sorted(attr_name_set), json_record_list)

    def _make_table_name(self):
        table_name = self._loader.make_table_name()

        return table_name.replace(tnt.KEY, self.__table_key)


class JsonTableFormatter(TableFormatter):

    def to_table_data(self):
        self._validate_source_data()

        converter = MultipleJsonTableConverter(self._source_data)
        converter.accept(self._loader)
        try:
            for tabledata in converter.to_table_data():
                yield tabledata
            return
        except ValidationError:
            pass

        converter = SingleJsonTableConverter(self._source_data)
        converter.accept(self._loader)

        for tabledata in converter.to_table_data():
            yield tabledata
