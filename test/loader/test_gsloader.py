# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

import pytest

import simplesqlite.loader as sloader


class Test_GoogleSheetsTableLoader_make_table_name:

    @property
    def monkey_property(self):
        return "testsheet"

    @pytest.mark.parametrize(["value", "title", "expected"], [
        ["%(sheet)s", "titlename", "testsheet"],
        ["%(title)s", "titlename", "titlename"],
        ["%(title)s", "table", "table"],
        [
            "prefix_%(title)s_%(sheet)s",
            "titlename",
            "prefix_titlename_testsheet"
        ],
        [
            "%(format_name)s%(format_id)s",
            "titlename",
            "spreadsheet0"
        ],
    ])
    def test_normal(self, monkeypatch, value, title, expected):
        loader = sloader.GoogleSheetsTableLoader("dummy")
        loader.table_name = value
        loader.title = title

        monkeypatch.setattr(
            sloader.GoogleSheetsTableLoader,
            "_sheet_name", self.monkey_property)

        assert loader.make_table_name() == expected

    @pytest.mark.parametrize(["value", "title", "expected"], [
        [None, "titlename", ValueError],
        ["", "titlename", ValueError],
        ["%(sheet)s", None, ValueError],
        ["%(sheet)s", "", ValueError],
    ])
    def test_exception(self, value, title, expected):
        loader = sloader.GoogleSheetsTableLoader("dummy")
        loader.table_name = value
        loader.title = title

        with pytest.raises(expected):
            loader.make_table_name()
