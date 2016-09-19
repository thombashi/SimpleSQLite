# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

import collections
import os

import pathvalidate
import pytest

import simplesqlite.loader as sloader
from simplesqlite.loader.data import TableData
from simplesqlite import InvalidTableNameError


Data = collections.namedtuple("Data", "value expected")

test_data_empty = Data(
    """[]""",
    [
        TableData("tmp", [], []),
    ])

test_data_01 = Data(
    """<table>
  <thead>
    <tr>
      <th>a</th>
      <th>b</th>
      <th>c</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td align="right">1</td>
      <td align="right">123.1</td>
      <td align="left">a</td>
    </tr>
    <tr>
      <td align="right">2</td>
      <td align="right">2.2</td>
      <td align="left">bb</td>
    </tr>
    <tr>
      <td align="right">3</td>
      <td align="right">3.3</td>
      <td align="left">ccc</td>
    </tr>
  </tbody>
</table>
""",
    [
        TableData(
            table_name=u"html1",
            header_list=[u'a', u'b', u'c'],
            record_list=[
                [u'1', u'123.1', u'a'],
                [u'2', u'2.2', u'bb'],
                [u'3', u'3.3', u'ccc'],
            ]
        ),
    ])

test_data_02 = Data(
    """<table id="tablename">
    <caption>caption</caption>
    <tr>
      <th>a</th>
      <th>b</th>
      <th>c</th>
    </tr>
    <tr>
      <td align="right">1</td>
      <td align="right">123.1</td>
      <td align="left">a</td>
    </tr>
    <tr>
      <td align="right">2</td>
      <td align="right">2.2</td>
      <td align="left">bb</td>
    </tr>
    <tr>
      <td align="right">3</td>
      <td align="right">3.3</td>
      <td align="left">ccc</td>
    </tr>
</table>
""",
    [
        TableData(
            table_name=u"tablename",
            header_list=[u'a', u'b', u'c'],
            record_list=[
                [u'1', u'123.1', u'a'],
                [u'2', u'2.2', u'bb'],
                [u'3', u'3.3', u'ccc'],
            ]
        ),
    ])

test_data_03 = Data(
    """
<html>
  <head>
    header
  </head>
  <body>
    hogehoge
  </body>
</html>
""",
    [])

test_data_04 = Data(
    """
<table id="tablename">
    <caption>caption</caption>
    <tr>
      <th>a</th>
      <th>b</th>
      <th>c</th>
    </tr>
    <tr>
      <td align="right">1</td>
      <td align="right">123.1</td>
      <td align="left">a</td>
    </tr>
    <tr>
      <td align="right">2</td>
      <td align="right">2.2</td>
      <td align="left">bb</td>
    </tr>
    <tr>
      <td align="right">3</td>
      <td align="right">3.3</td>
      <td align="left">ccc</td>
    </tr>
</table>
<table>
    <tr>
      <th>a</th>
      <th>b</th>
    </tr>
    <tr>
      <td align="right">1</td>
      <td align="right">123.1</td>
    </tr>
    <tr>
      <td align="right">2</td>
      <td align="right">2.2</td>
    </tr>
    <tr>
      <td align="right">3</td>
      <td align="right">3.3</td>
    </tr>
</table>
""",
    [
        TableData(
            table_name=u"tmp_tablename",
            header_list=[u'a', u'b', u'c'],
            record_list=[
                [u'1', u'123.1', u'a'],
                [u'2', u'2.2', u'bb'],
                [u'3', u'3.3', u'ccc'],
            ]
        ),
        TableData(
            table_name=u"tmp_html2",
            header_list=[u'a', u'b'],
            record_list=[
                [u'1', u'123.1'],
                [u'2', u'2.2'],
                [u'3', u'3.3'],
            ]
        ),
    ])


class Test_HtmlTableFileLoader_make_table_name:

    @pytest.mark.parametrize(["value", "source", "expected"], [
        ["%(filename)s", "/path/to/data.html", "data"],
        ["prefix_%(filename)s", "/path/to/data.html", "prefix_data"],
        ["%(filename)s_suffix", "/path/to/data.html", "data_suffix"],
        [
            "prefix_%(filename)s_suffix",
            "/path/to/data.html",
            "prefix_data_suffix"
        ],
        [
            "%(filename)s%(filename)s",
            "/path/to/data.html",
            "datadata"
        ],
        [
            "%(format_name)s%(format_id)s_%(filename)s",
            "/path/to/data.html",
            "html0_data"
        ],
    ])
    def test_normal(self, value, source, expected):
        loader = sloader.HtmlTableFileLoader(source)
        loader.table_name = value

        assert loader.make_table_name() == expected

    @pytest.mark.parametrize(["value", "source", "expected"], [
        [None, "/path/to/data.html", ValueError],
        ["", "/path/to/data.html", ValueError],
        ["%(filename)s", None, ValueError],
        ["%(filename)s", "", ValueError],
        [
            "%(%(filename)s)",
            "/path/to/data.html",
            InvalidTableNameError  # %(data)
        ],
        [
            "%(key)s_%(filename)s",
            "/path/to/data.html",
            InvalidTableNameError  # "%(key)s_data"
        ],
    ])
    def test_exception(self, value, source, expected):
        loader = sloader.HtmlTableFileLoader(source)
        loader.table_name = value

        with pytest.raises(expected):
            loader.make_table_name()


class Test_HtmlTableFileLoader_load:

    @pytest.mark.parametrize(
        [
            "table_text",
            "filename",
            "table_name",
            "expected_tabletuple_list",
        ],
        [
            [
                test_data_01.value,
                "tmp.html",
                "%(key)s",
                test_data_01.expected
            ],
            [
                test_data_02.value,
                "tmp.html",
                "%(key)s",
                test_data_02.expected,
            ],
            [
                test_data_03.value,
                "tmp.html",
                "%(default)s",
                test_data_03.expected,
            ],
            [
                test_data_04.value,
                "tmp.html",
                "%(default)s",
                test_data_04.expected,
            ],
        ])
    def test_normal(
            self, tmpdir, table_text, filename,
            table_name, expected_tabletuple_list):
        p_file_path = tmpdir.join(filename)

        parent_dir_path = os.path.dirname(str(p_file_path))
        if not os.path.isdir(parent_dir_path):
            os.makedirs(parent_dir_path)

        with open(str(p_file_path), "w") as f:
            f.write(table_text)

        loader = sloader.HtmlTableFileLoader(str(p_file_path))
        loader.table_name = table_name

        for tabletuple, expected in zip(loader.load(), expected_tabletuple_list):
            assert tabletuple == expected

    @pytest.mark.parametrize(
        [
            "table_text",
            "filename",
            "expected",
        ],
        [
            [
                "",
                "tmp.html",
                sloader.InvalidDataError,
            ],
        ])
    def test_exception(
            self, tmpdir, table_text, filename, expected):
        p_file_path = tmpdir.join(filename)

        with open(str(p_file_path), "w") as f:
            f.write(table_text)

        loader = sloader.HtmlTableFileLoader(str(p_file_path))

        with pytest.raises(expected):
            for _tabletuple in loader.load():
                pass

    @pytest.mark.parametrize(["filename", "expected"], [
        ["", sloader.InvalidDataError],
        [None, sloader.InvalidDataError],
    ])
    def test_null(
            self, tmpdir, filename, expected):
        loader = sloader.HtmlTableFileLoader(filename)

        with pytest.raises(expected):
            for _tabletuple in loader.load():
                pass


class Test_HtmlTableTextLoader_make_table_name:

    @pytest.mark.parametrize(["value", "expected"], [
        ["%(format_name)s%(format_id)s", "html0"],
        ["tablename", "tablename"],
        ["table", "table_html"],
    ])
    def test_normal(self, value, expected):
        loader = sloader.HtmlTableTextLoader("dummy")
        loader.table_name = value

        assert loader.make_table_name() == expected

    @pytest.mark.parametrize(["value", "source", "expected"], [
        ["<table></table>", "%(filename)s", InvalidTableNameError],
        ["<table></table>", "%(key)s", InvalidTableNameError],
        [None, "tablename", ValueError],
        ["", "tablename", ValueError],
    ])
    def test_exception(self, value, source, expected):
        loader = sloader.HtmlTableTextLoader(source)
        loader.table_name = value

        with pytest.raises(expected):
            loader.make_table_name()


class Test_HtmlTableTextLoader_load:

    @pytest.mark.parametrize(
        [
            "table_text",
            "table_name",
            "expected_tabletuple_list",
        ],
        [
            [
                test_data_01.value,
                "%(default)s",
                test_data_01.expected,
            ],
            [
                test_data_02.value,
                "%(default)s",
                test_data_02.expected,
            ],
            [
                test_data_03.value,
                "%(default)s",
                test_data_03.expected,
            ],
        ])
    def test_normal(self, table_text, table_name, expected_tabletuple_list):
        loader = sloader.HtmlTableTextLoader(table_text)
        loader.table_name = table_name

        for tabletuple in loader.load():
            assert tabletuple in expected_tabletuple_list

    @pytest.mark.parametrize(["table_text", "expected"], [
        [
            "",
            sloader.InvalidDataError,
        ],
    ])
    def test_exception(self, table_text, expected):
        loader = sloader.HtmlTableTextLoader(table_text)
        loader.table_name = "dummy"

        with pytest.raises(expected):
            for _tabletuple in loader.load():
                pass

    @pytest.mark.parametrize(["table_text", "expected"], [
        ["", sloader.InvalidDataError],
        [None, sloader.InvalidDataError],
    ])
    def test_null(self, table_text, expected):
        loader = sloader.HtmlTableTextLoader(table_text)
        loader.table_name = "dummy"

        with pytest.raises(expected):
            for _tabletuple in loader.load():
                pass
