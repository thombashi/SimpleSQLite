# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import unicode_literals
import os
import re
import string

import pytest

from simplesqlite.sqlquery import SqlQuery
from simplesqlite import SqlSyntaxError

nan = float("nan")
inf = float("inf")


class Test_SqlQuery_sanitize:
    SANITIZE_CHAR_LIST = [
        "%", "/", "(", ")", "[", "]", "<", ">", ".", ";",
        "'", '"', "!", "\\", "#", " ", "-", "+", "=", "\n"
    ]

    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            ["AAA{:s}".format(re.escape(c)), "AAA"] for c in SANITIZE_CHAR_LIST
        ] + [
            ["{:s}BBB".format(re.escape(c)), "BBB"] for c in SANITIZE_CHAR_LIST
        ] + [
            [
                "%a/b(c)d[e]f<g>h.i;j'k!l\\m#n _o-p+q=r\nstrvwxyz" +
                os.linesep,
                "abcdefghijklmn_opqrstrvwxyz"
            ]
        ]
    )
    def test_normal(self, value, expected):
        assert SqlQuery.sanitize(value) == expected

    @pytest.mark.parametrize(["value", "expected"], [
        [None, TypeError],
        [1, TypeError],
        [nan, TypeError],
        [True, TypeError],
    ])
    def test_abnormal(self, value, expected):
        with pytest.raises(expected):
            SqlQuery.sanitize(value)


class Test_SqlQuery_to_table_str:

    @pytest.mark.parametrize(["value", "expected"], [
        ["test", "test"],
        ["te%st", "[te%st]"],
        ["te(st", "[te(st]"],
        ["te)st", "[te)st]"],
        ["te-st", "[te-st]"],
        ["te+st", "[te+st]"],
        ["te.st", "[te.st]"],
        ["te st", "'te st'"],
    ])
    def test_normal(self, value, expected):
        assert SqlQuery.to_table_str(value) == expected

    @pytest.mark.parametrize(["value", "expected"], [
        [None, TypeError],
        [1, TypeError],
        [False, TypeError],
    ])
    def test_exception(self, value, expected):
        with pytest.raises(expected):
            assert SqlQuery.to_table_str(value)


class Test_SqlQuery_to_attr_str:

    @pytest.mark.parametrize(["value", "operation", "expected"], [
        ["test", None, "test"],
        ["te'st", None, '"te_st"'],
        ['te"st', None, '"te_st"'],
        ["test", "AVG", "AVG(test)"],
        ["attr_a", 2, '"attr_a"'],
        ["attr_a", True, '"attr_a"'],
    ] + [
        [
            "te{:s}st".format(re.escape(c)),
            None,
            "[te{:s}st]".format(re.escape(c)),
        ]
        for c in string.digits + "%(). -+#"
    ]
    )
    def test_normal(self, value, operation, expected):
        assert SqlQuery.to_attr_str(value, operation) == expected

    @pytest.mark.parametrize(["value", "expected"], [
        [None, TypeError],
        [1, TypeError],
        [False, TypeError],
    ])
    def test_exception_1(self, value, expected):
        with pytest.raises(expected):
            SqlQuery.to_attr_str(value)


class Test_SqlQuery_to_attr_str_list:

    @pytest.mark.parametrize(
        ["value", "operation", "expected"],
        [
            [
                ["%aaa", "bbb", "ccc-ddd"],
                None,
                ["[%aaa]", "bbb", "[ccc-ddd]"],
            ],
            [
                ["%aaa", "bbb"],
                "SUM",
                ["SUM([%aaa])", "SUM(bbb)"],
            ],
            [[], None, []],
        ]
    )
    def test_normal(self, value, operation, expected):
        assert list(SqlQuery.to_attr_str_list(value, operation)) == expected

    @pytest.mark.parametrize(["value", "expected"], [
        [None, TypeError],
        [nan, TypeError],
        [True, TypeError],
    ])
    def test_exception(self, value, expected):
        with pytest.raises(expected):
            SqlQuery.to_attr_str_list(value)


class Test_SqlQuery_to_value_str:

    @pytest.mark.parametrize(["value", "expected"], [
        [0, "0"],
        ["0", "0"],
        [1.1, "1.1"],
        ["test", "'test'"],
        ["te st", "'te st'"],
        [None, "NULL"],
        [False, "'False'"],
    ])
    def test_normal(self, value, expected):
        assert SqlQuery.to_value_str(value) == expected

    @pytest.mark.parametrize(["value", "expected"], [
        [nan, "nan"],
        [inf, "inf"],
    ])
    def test_abnormal(self, value, expected):
        assert SqlQuery.to_value_str(value) == expected


class Test_SqlQuery_to_value_str_list:

    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            [[0, "bbb", None], ["0", "'bbb'", "NULL"]],
            [[], []],
        ]
    )
    def test_normal(self, value, expected):
        assert list(SqlQuery.to_value_str_list(value)) == expected

    @pytest.mark.parametrize(["value", "expected"], [
        [None, TypeError],
        [nan, TypeError],
    ])
    def test_exception(self, value, expected):
        with pytest.raises(expected):
            assert SqlQuery.to_value_str_list(value)


class Test_SqlQuery_make_select:

    @pytest.mark.parametrize(
        ["select", "table", "where", "extra", "expected"],
        [
            [
                "A", "B", None, None,
                "SELECT A FROM B"
            ],
            [
                "A", "B B", None, None,
                "SELECT A FROM 'B B'"
            ],
            [
                "A", "B-B", SqlQuery.make_where("C", 1), None,
                "SELECT A FROM [B-B] WHERE C = 1"
            ],
            [
                "A", "B-B", SqlQuery.make_where("C", 1, ">"), "ORDER BY D",
                "SELECT A FROM [B-B] WHERE C > 1 ORDER BY D"
            ],
        ])
    def test_normal(self, select, table, where, extra, expected):
        assert SqlQuery.make_select(select, table, where, extra) == expected

    @pytest.mark.parametrize(
        ["select", "table", "where", "extra", "expected"], [
            ["A", None, None, None, ValueError],
            ["", "B", None, None, ValueError],
            [None, None, None, None, ValueError],
            [None, "B", None, None, ValueError],
            [nan, None, None, None, ValueError],
            [nan, nan, None, None, ValueError],
            [nan, nan, nan, None, ValueError],
            [nan, nan, nan, nan, ValueError],
        ])
    def test_exception(self, select, table, where, extra, expected):
        with pytest.raises(expected):
            SqlQuery.make_select(select, table, where, extra)


class Test_SqlQuery_make_insert:

    @pytest.mark.parametrize(
        ["table", "insert_tuple", "expected"], [
            [
                "A", ["B"],
                "INSERT INTO A VALUES (?)"
            ],
            [
                "A", ["B", "C"],
                "INSERT INTO A VALUES (?,?)"
            ],
            [
                "A", ["AAAA", 2, 0.1],
                "INSERT INTO A VALUES (?,?,?)"
            ],
        ])
    def test_normal(self, table, insert_tuple, expected):
        assert SqlQuery.make_insert(table, insert_tuple) == expected

    @pytest.mark.parametrize(
        ["table", "insert_tuple", "is_isert_many", "expected"], [
            ["", [], False, ValueError],
            ["", None, True, ValueError],
            ["", ["B"], False, ValueError],
            ["A", [], True, ValueError],
            ["A", None, False, ValueError],
            [None, None, True, ValueError],
            [None, ["B"], False, ValueError],
            [None, [], True, ValueError],
        ])
    def test_exception(self, table, insert_tuple, is_isert_many, expected):
        with pytest.raises(expected):
            SqlQuery.make_insert(table, insert_tuple)


class Test_SqlQuery_make_update:

    @pytest.mark.parametrize(
        ["table", "set_query", "where", "expected"], [
            [
                "A", "B=1", None,
                "UPDATE A SET B=1"
            ],
            [
                "A", "B=1", SqlQuery.make_where("C", 1, ">"),
                "UPDATE A SET B=1 WHERE C > 1"
            ],
        ])
    def test_normal(self, table, set_query, where, expected):
        assert SqlQuery.make_update(table, set_query, where) == expected

    @pytest.mark.parametrize(
        ["table", "set_query", "where", "expected"], [
            [None, "B=1", None, ValueError],
            ["", "B=1", None, ValueError],
            ["A", None, None, ValueError],
            ["A", "", None, ValueError],
        ])
    def test_exception(self, table, set_query, where, expected):
        with pytest.raises(expected):
            SqlQuery.make_update(table, set_query, where)


class Test_SqlQuery_make_where:

    @pytest.mark.parametrize(["key", "value", "operation", "expected"], [
        ["tkey", "tvalue", "=", "tkey = 'tvalue'"],
        ["key key", "tvalue", "!=", "[key key] != 'tvalue'"],
        ["%key+key", 100, "<", "[%key+key] < 100"],
        ["key.key", "555", ">", "[key.key] > 555"],

        ["tkey", None, "=", "tkey IS NULL"],
        ["tkey", None, "!=", "tkey IS NOT NULL"],
    ])
    def test_normal(self, key, value, operation, expected):
        assert SqlQuery.make_where(key, value, operation) == expected

    @pytest.mark.parametrize(["key", "value", "operation", "expected"], [
        ["tkey", "tvalue", None, SqlSyntaxError],
        ["tkey", "tvalue", "INVALID_VALUE", SqlSyntaxError],
    ])
    def test_exception(self, key, value, operation, expected):
        with pytest.raises(expected):
            SqlQuery.make_where(key, value, operation)


class Test_SqlQuery_make_where_in:

    @pytest.mark.parametrize(["key", "value", "expected"], [
        ["key", ["attr_a", "attr_b"], "key IN ('attr_a', 'attr_b')"],
    ])
    def test_normal(self, key, value, expected):
        assert SqlQuery.make_where_in(key, value) == expected

    @pytest.mark.parametrize(["key", "value", "expected"], [
        ["key", None, TypeError],
        ["key", 1, TypeError],
        [None, ["attr_a", "attr_b"], TypeError],
        [None, None, TypeError],
    ])
    def test_exception(self, key, value, expected):
        with pytest.raises(expected):
            SqlQuery.make_where_in(key, value)


class Test_SqlQuery_make_where_not_in:

    @pytest.mark.parametrize(["key", "value", "expected"], [
        ["key", ["attr_a", "attr_b"], "key NOT IN ('attr_a', 'attr_b')"],
    ])
    def test_normal(self, key, value, expected):
        assert SqlQuery.make_where_not_in(key, value) == expected

    @pytest.mark.parametrize(["key", "value", "expected"], [
        ["key", None, TypeError],
        ["key", 1, TypeError],
        [None, ["attr_a", "attr_b"], TypeError],
        [None, None, TypeError],
    ])
    def test_exception(self, key, value, expected):
        with pytest.raises(expected):
            SqlQuery.make_where_not_in(key, value)
