# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


from __future__ import absolute_import

try:
    import json
except ImportError:
    import simplejson as json

import path

from ..interface import TableLoader
from .formatter import JsonTableFormatter


class JsonTableFileLoader(TableLoader):
    """
    Concrete class of JSON file loader.
    """

    def __init__(self, file_path=None):
        super(JsonTableFileLoader, self).__init__(file_path)
        self.table_name = "%(default)s"

    def make_table_name(self):
        self._validate()

        return self.table_name.replace(
            "%(filename)s", path.Path(self.source).namebase)

    def load(self):
        """
        Load a JSON file from :py:attr:`.source` that includes table data.
        First, single table data in a file,
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

        Second, multiple table data in a file,
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

        The table name string is making from
        :py:attr:`~simplesqlite.loader.interface.TableLoader.table_name`.
        Following format specifiers are replaced with specific string.

            +----------------+------------------------------------------------+
            |format specifier|value after the replacement                     |
            +================+================================================+
            |``%(filename)s``|Filename. Defaults to single JSON table.        |
            +----------------+------------------------------------------------+
            |``%(key)s``     |Key of the table data                           |
            |                |(only for multiple JSON table).                 |
            |                |Defaults to multiple JSON table.                |
            +----------------+------------------------------------------------+

        :return: Loaded table data.
        :rtype: iterator of |TableData|
        """

        self._validate()

        with open(self.source, "r") as fp:
            json_buffer = json.load(fp)

        formatter = JsonTableFormatter(json_buffer)
        formatter.accept(self)

        return formatter.to_table_data()


class JsonTableTextLoader(TableLoader):

    def __init__(self, text):
        super(JsonTableTextLoader, self).__init__(text)
        self.table_name = "%(default)s"

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
