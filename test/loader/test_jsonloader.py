# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""


import collections
import os

import pytest

import simplesqlite.loader as sloader
from simplesqlite.loader.data import TableData


Data = collections.namedtuple("Data", "value expected")

test_data_empty = Data(
    """[]""",
    [
        TableData("tmp", [], []),
    ])

test_data_01 = Data(
    """[
        {"attr_b": 4, "attr_c": "a", "attr_a": 1},
        {"attr_b": 2.1, "attr_c": "bb", "attr_a": 2},
        {"attr_b": 120.9, "attr_c": "ccc", "attr_a": 3}
    ]""",
    [
        TableData(
            "tmp",
            ["attr_a", "attr_b", "attr_c"],
            [
                {u'attr_a': 1, u'attr_b': 4, u'attr_c': u'a'},
                {u'attr_a': 2, u'attr_b': 2.1, u'attr_c': u'bb'},
                {u'attr_a': 3, u'attr_b': 120.9,
                    u'attr_c': u'ccc'},
            ]
        ),
    ])

test_data_02 = Data(
    """[
        {"attr_a": 1},
        {"attr_b": 2.1, "attr_c": "bb"}
    ]""",
    [
        TableData(
            "tmp",
            ["attr_a", "attr_b", "attr_c"],
            [
                {u'attr_a': 1},
                {u'attr_b': 2.1, u'attr_c': u'bb'},
            ]
        ),
    ])

test_data_03 = Data(
    """{
        "table_a" : [
            {"attr_b": 4, "attr_c": "a", "attr_a": 1},
            {"attr_b": 2.1, "attr_c": "bb", "attr_a": 2},
            {"attr_b": 120.9, "attr_c": "ccc", "attr_a": 3}
        ],
        "table_b" : [
            {"a": 1, "b": 4},
            {"a": 2 },
            {"a": 3, "b": 120.9}
        ]
    }""",
    [
        TableData(
            u"table_a",
            [u"attr_a", u"attr_b", u"attr_c"],
            [
                {u'attr_a': 1, u'attr_b': 4, u'attr_c': u'a'},
                {u'attr_a': 2, u'attr_b': 2.1, u'attr_c': u'bb'},
                {u'attr_a': 3, u'attr_b': 120.9,
                    u'attr_c': u'ccc'},
            ]
        ),
        TableData(
            u"table_b",
            [u"a", u"b"],
            [
                {u'a': 1, u'b': 4},
                {u'a': 2, },
                {u'a': 3, u'b': 120.9},
            ]
        ),
    ])

test_data_04 = Data(
    """{
        "table_a" : [
            {"attr_b": 4, "attr_c": "a", "attr_a": 1},
            {"attr_b": 2.1, "attr_c": "bb", "attr_a": 2},
            {"attr_b": 120.9, "attr_c": "ccc", "attr_a": 3}
        ],
        "table_b" : [
            {"a": 1, "b": 4},
            {"a": 2 },
            {"a": 3, "b": 120.9}
        ]
    }""",
    [
        TableData(
            u"tmp_table_a",
            [u"attr_a", u"attr_b", u"attr_c"],
            [
                {u'attr_a': 1, u'attr_b': 4, u'attr_c': u'a'},
                {u'attr_a': 2, u'attr_b': 2.1, u'attr_c': u'bb'},
                {u'attr_a': 3, u'attr_b': 120.9,
                    u'attr_c': u'ccc'},
            ]
        ),
        TableData(
            u"tmp_table_b",
            [u"a", u"b"],
            [
                {u'a': 1, u'b': 4},
                {u'a': 2, },
                {u'a': 3, u'b': 120.9},
            ]
        ),
    ])


class Test_JsonTableFileLoader_make_table_name:

    @pytest.mark.parametrize(["value", "source", "expected"], [
        ["%(filename)s", "/path/to/data.json", "data"],
        ["prefix_%(filename)s", "/path/to/data.json", "prefix_data"],
        ["%(filename)s_suffix", "/path/to/data.json", "data_suffix"],
        [
            "prefix_%(filename)s_suffix",
            "/path/to/data.json",
            "prefix_data_suffix"
        ],
        [
            "%(filename)s%(filename)s",
            "/path/to/data.json",
            "datadata"
        ],
        ["%(%(filename)s)", "/path/to/data.json", "%(data)"],
    ])
    def test_normal(self, value, source, expected):
        loader = sloader.JsonTableFileLoader(source)
        loader.table_name = value

        assert loader.make_table_name() == expected

    @pytest.mark.parametrize(["value", "source", "expected"], [
        [None, "/path/to/data.json", ValueError],
        ["", "/path/to/data.json", ValueError],
        ["%(filename)s", None, ValueError],
        ["%(filename)s", "", ValueError],
    ])
    def test_exception(self, value, source, expected):
        loader = sloader.JsonTableFileLoader(source)
        loader.table_name = value

        with pytest.raises(expected):
            loader.make_table_name()


class Test_JsonTableFileLoader_load:

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
                "tmp.json",
                "%(default)s",
                test_data_01.expected
            ],
            [
                test_data_02.value,
                "tmp.json",
                "%(default)s",
                test_data_02.expected
            ],
            [
                test_data_03.value,
                "tmp.json",
                "%(default)s",
                test_data_03.expected
            ],
            [
                test_data_04.value,
                "tmp.json",
                "%(filename)s_%(key)s",
                test_data_04.expected
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

        loader = sloader.JsonTableFileLoader(str(p_file_path))
        loader.table_name = table_name

        for tabletuple in loader.load():
            assert tabletuple in expected_tabletuple_list

    @pytest.mark.parametrize(
        [
            "table_text",
            "filename",
            "expected",
        ],
        [
            [
                "[]",
                "tmp.json",
                sloader.InvalidDataError,
            ],
            [
                """[
                    {"attr_b": 4, "attr_c": "a", "attr_a": {"aaa": 1}}
                ]""",
                "tmp.json",
                sloader.ValidationError,
            ],
        ])
    def test_exception(
            self, tmpdir, table_text, filename, expected):
        p_file_path = tmpdir.join(filename)

        with open(str(p_file_path), "w") as f:
            f.write(table_text)

        loader = sloader.JsonTableFileLoader(str(p_file_path))

        with pytest.raises(expected):
            for _tabletuple in loader.load():
                pass

    @pytest.mark.parametrize(["filename", "expected"], [
        ["", ValueError],
        [None, ValueError],
    ])
    def test_null(
            self, tmpdir, filename, expected):
        loader = sloader.JsonTableFileLoader(filename)

        with pytest.raises(expected):
            for _tabletuple in loader.load():
                pass


class Test_JsonTableTextLoader_make_table_name:

    @pytest.mark.parametrize(["value", "expected"], [
        ["%(filename)s", "%(filename)s"],
        ["tablename", "tablename"],
    ])
    def test_normal(self, value, expected):
        loader = sloader.JsonTableTextLoader("dummy")
        loader.table_name = value

        assert loader.make_table_name() == expected

    @pytest.mark.parametrize(["value", "source", "expected"], [
        [None, "tablename", ValueError],
        ["", "tablename", ValueError],
    ])
    def test_exception(self, value, source, expected):
        loader = sloader.JsonTableTextLoader(source)
        loader.table_name = value

        with pytest.raises(expected):
            loader.make_table_name()


class Test_JsonTableTextLoader_load:

    @pytest.mark.parametrize(
        [
            "table_text",
            "table_name",
            "expected_tabletuple_list",
        ],
        [
            [
                test_data_01.value,
                "tmp",
                test_data_01.expected,
            ],
            [
                test_data_02.value,
                "tmp",
                test_data_02.expected,
            ],
            [
                test_data_03.value,
                "%(default)s",
                test_data_03.expected
            ],
        ])
    def test_normal(self, table_text, table_name, expected_tabletuple_list):
        loader = sloader.JsonTableTextLoader(table_text)
        loader.table_name = table_name

        for tabletuple in loader.load():
            assert tabletuple in expected_tabletuple_list

    @pytest.mark.parametrize(["table_text", "expected"], [
        [
            "[]",
            sloader.InvalidDataError,
        ],
        [
            """[
                {"attr_b": 4, "attr_c": "a", "attr_a": {"aaa": 1}}
            ]""",
            sloader.ValidationError,
        ],
    ])
    def test_exception(self, table_text, expected):
        loader = sloader.JsonTableTextLoader(table_text)
        loader.table_name = "dummy"

        with pytest.raises(expected):
            for _tabletuple in loader.load():
                pass

    @pytest.mark.parametrize(["table_text", "expected"], [
        ["", ValueError],
        [None, ValueError],
    ])
    def test_null(self, table_text, expected):
        loader = sloader.JsonTableTextLoader(table_text)
        loader.table_name = "dummy"

        with pytest.raises(expected):
            for _tabletuple in loader.load():
                pass
