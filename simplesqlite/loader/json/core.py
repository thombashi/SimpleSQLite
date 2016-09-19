# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import
import json

from ..constant import TableNameTemplate as tnt
from ..interface import TableLoader
from .formatter import JsonTableFormatter


class JsonTableLoader(TableLoader):
    """
    Abstract class of JSON table loader.
    """

    @property
    def format_name(self):
        return "json"


class JsonTableFileLoader(JsonTableLoader):
    """
    Concrete class of JSON file loader.

    .. py:attribute:: table_name

        Table name string. Defaults to ``%(filename)s_%(key)s``.
    """

    def __init__(self, file_path=None):
        super(JsonTableFileLoader, self).__init__(file_path)

    def make_table_name(self):
        return self._sanitize_table_name(self._make_file_table_name())

    def load(self):
        """
        Load a JSON file from :py:attr:`.source` that includes table data.
        This method can be loading two types of JSON formats:
        **(1)** single table data in a file,
        acceptable JSON schema is as follows:

        .. code-block:: json

            {
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

        :Examples:

            :ref:`example-convert-single-json-table`

        **(2)** multiple table data in a file,
        acceptable JSON schema is as follows:

        .. code-block:: json

            {
                "type": "object",
                "additionalProperties": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": {
                            "anyOf": [
                                {"type": "string"},
                                {"type": "number"},
                                {"type": "null"}
                            ]
                        }
                    }
                }
            }

        :Examples:

            :ref:`example-convert-multi-json-table`

        The table name is determined by the value of
        :py:attr:`~JsonTableFileLoader.table_name`.
        Following format specifiers are replaced with specific string.

            ===================  ==============================================
            format specifier     value after the replacement
            ===================  ==============================================
            ``%(filename)s``     Filename. Defaults to single JSON table.
            ``%(key)s``          | This is replaced the different value
                                 | for each single/multipl JSON tables:
                                 | [single JSON table]
                                 | ``%(format_name)s%(format_id)s``
                                 | [multiple JSON table] Key of the table data.
            ``%(format_name)s``  ``json``
            ``%(format_id)s``    unique number in the same format
            ``%(global_id)s``    unique number in all of the format
            ===================  ==============================================

        :return: Loaded table data.
        :rtype: iterator of |TableData|
        :raises simplesqlite.loader.InvalidDataError:
            If the data is invalid JSON.
        :raises simplesqlite.loader.ValidationError:
            If the data is not acceptable JSON format.
        """

        self._validate()

        with open(self.source, "r") as fp:
            json_buffer = json.load(fp)

        formatter = JsonTableFormatter(json_buffer)
        formatter.accept(self)

        return formatter.to_table_data()

    def _get_default_table_name_template(self):
        return "{:s}_{:s}".format(tnt.FILENAME, tnt.KEY)


class JsonTableTextLoader(JsonTableLoader):
    """
    Concrete class of JSON text loader.

    .. py:attribute:: table_name

        Table name string. Defaults to ``%(key)s``.
    """

    def __init__(self, text):
        super(JsonTableTextLoader, self).__init__(text)

    def load(self):
        """
        Load a JSON text from :py:attr:`.source` that includes table data.

        :return: Loaded table data.
        :rtype: iterator of |TableData|

        .. seealso:: :py:meth:`.JsonTableFileLoader.load`
        """

        self._validate()

        json_buffer = json.loads(self.source)

        formatter = JsonTableFormatter(json_buffer)
        formatter.accept(self)

        return formatter.to_table_data()

    def _get_default_table_name_template(self):
        return "{:s}".format(tnt.KEY)
