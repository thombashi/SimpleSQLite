# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import absolute_import

from ..constant import TableNameTemplate as tnt
from ..interface import TableLoader
from .formatter import HtmlTableFormatter


class HtmlTableLoader(TableLoader):
    """
    Abstract class of HTML table loader.
    """

    @property
    def format_name(self):
        return "html"


class HtmlTableFileLoader(HtmlTableLoader):
    """
    Concrete class of HTML file loader.

    .. py:attribute:: table_name

        Table name string. Defaults to ``%(filename)s_%(key)s``.
    """

    def __init__(self, file_path=None):
        super(HtmlTableFileLoader, self).__init__(file_path)

    def make_table_name(self):
        """
        |make_table_name|

        :return: Table name.
        :rtype: str
        """

        return self._make_file_table_name()

    def load(self):
        """
        Load a HTML file from :py:attr:`.source` that includes table data.

        The table name is determined by the value of
        :py:attr:`~HtmlTableFileLoader.table_name`.
        Following format specifiers are replaced with specific string.

            ===================  ==============================================
            format specifier     value after the replacement
            ===================  ==============================================
            ``%(filename)s``     filename
            ``%(key)s``          | This is replaced to :
                                 | **(1)** ``id`` attribute of the table tag
                                 | **(2)** ``%(format_name)s%(format_id)s``
                                 | if ``id`` attribute is not included
                                 | the table tag..
            ``%(format_name)s``  ``html``
            ``%(format_id)s``    unique number in the same format
            ``%(global_id)s``    unique number in all of the format
            ===================  ==============================================

        :return: Loaded table data.
        :rtype: iterator of |TableData|
        :raises simplesqlite.loader.InvalidDataError:
            If the HTML data is invalid.
        """

        self._validate()

        formatter = None
        with open(self.source, "r") as fp:
            formatter = HtmlTableFormatter(fp.read())
        formatter.accept(self)

        return formatter.to_table_data()

    def _get_default_table_name_template(self):
        return "{:s}_{:s}".format(tnt.FILENAME, tnt.KEY)


class HtmlTableTextLoader(HtmlTableLoader):
    """
    Concrete class of HTML text loader.

    .. py:attribute:: table_name

        Table name string. Defaults to ``%(key)s``.
    """

    def __init__(self, text):
        super(HtmlTableTextLoader, self).__init__(text)

    def load(self):
        """
        Load a HTML text from :py:attr:`.source` that includes table data.

        :return: Loaded table data.
        :rtype: iterator of |TableData|
        :raises simplesqlite.loader.InvalidDataError:
            If the HTML data is invalid.

        .. seealso:: :py:meth:`.HtmlTableFileLoader.load`
        """

        self._validate()

        formatter = HtmlTableFormatter(self.source)
        formatter.accept(self)

        return formatter.to_table_data()

    def _get_default_table_name_template(self):
        return "{:s}".format(tnt.KEY)
