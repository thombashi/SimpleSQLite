# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import unicode_literals

import os
import re
import string

import pytest
import six
from simplesqlite import NameValidationError, SqlSyntaxError
from simplesqlite.query import And, Attr, AttrList, Select, Table, Value, Where, make_index_name


nan = float("nan")
inf = float("inf")


def assert_query_item(item, expected):
    assert item.to_query() == expected
    assert six.text_type(item) == expected
    assert "{}".format(item) == expected


class Test_Table(object):
    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            ["test", "test"],
            ["te%st", "[te%st]"],
            ["te(st", "[te(st]"],
            ["te)st", "[te)st]"],
            ["te-st", "[te-st]"],
            ["te+st", "[te+st]"],
            ["te.st", "[te.st]"],
            ["te,st", "[te,st]"],
            ["te st", "'te st'"],
        ],
    )
    def test_normal(self, value, expected):
        assert_query_item(Table(value), expected)

    @pytest.mark.parametrize(
        ["value", "expected"], [[None, TypeError], [1, TypeError], [False, TypeError]]
    )
    def test_exception(self, value, expected):
        with pytest.raises(expected):
            assert "{}".format(Table(value))


class Test_Attr(object):

    RESERVED_KEYWORDS = [
        "ADD",
        "ALL",
        "ALTER",
        "AND",
        "AS",
        "AUTOINCREMENT",
        "BETWEEN",
        "CASE",
        "CHECK",
        "COLLATE",
        "COMMIT",
        "CONSTRAINT",
        "CREATE",
        "DEFAULT",
        "DEFERRABLE",
        "DELETE",
        "DISTINCT",
        "DROP",
        "ELSE",
        "ESCAPE",
        "EXCEPT",
        "EXISTS",
        "FOREIGN",
        "FROM",
        "GROUP",
        "HAVING",
        "IN",
        "INDEX",
        "INSERT",
        "INTERSECT",
        "INTO",
        "IS",
        "ISNULL",
        "JOIN",
        "LIMIT",
        "NOT",
        "NOTNULL",
        "NULL",
        "ON",
        "OR",
        "ORDER",
        "PRIMARY",
        "REFERENCES",
        "SELECT",
        "SET",
        "TABLE",
        "THEN",
        "TO",
        "TRANSACTION",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USING",
        "VALUES",
        "WHEN",
        "WHERE",
    ]

    @pytest.mark.parametrize(
        ["value", "operation", "expected"],
        [
            ["test", None, "test"],
            ["te'st", None, '"te_st"'],
            ['te"st', None, '"te_st"'],
            ["te,st", None, '"te_st"'],
            ["te\nst", None, '"te_st"'],
            ["t\ne\ns\r\nt", None, '"t_e_s__t"'],
            ["te.st", None, "[te.st]"],
            ["t[es]t", None, '"t[es]t"'],
            ["test", "AVG", "AVG(test)"],
            ["attr_a", 2, '"attr_a"'],
            ["attr_a", True, '"attr_a"'],
            ["ABCD>8.5", None, "[ABCD>8.5]"],
            ["k@l[m]n{o}p;q:r,s.t/u", None, '"k@l[m]n{o}p;q:r_s.t/u"'],
        ]
        + [
            ["te{:s}st".format(re.escape(c)), None, "[te{:s}st]".format(re.escape(c))]
            for c in string.digits + "%(). -+#"
        ],
    )
    def test_normal(self, value, operation, expected):
        assert_query_item(Attr(value, operation), expected)

    @pytest.mark.parametrize(
        ["value", "expected"], [[word, '"{}"'.format(word)] for word in RESERVED_KEYWORDS]
    )
    def test_normal_reserved(self, value, expected):
        assert_query_item(Attr(value), expected)

    @pytest.mark.parametrize(
        ["value", "expected"], [[None, TypeError], [1, TypeError], [False, TypeError]]
    )
    def test_exception_1(self, value, expected):
        with pytest.raises(expected):
            "{}".format(Attr(value))


class Test_AttrList(object):
    @pytest.mark.parametrize(
        ["value", "operation", "expected"],
        [
            [["%aaa", "bbb", "ccc-ddd"], None, "[%aaa],bbb,[ccc-ddd]"],
            [["%aaa", "bbb"], "SUM", "SUM([%aaa]),SUM(bbb)"],
            [[], None, ""],
        ],
    )
    def test_normal(self, value, operation, expected):
        assert_query_item(AttrList(value, operation), expected)

    @pytest.mark.parametrize(
        ["value", "expected"], [[None, TypeError], [nan, TypeError], [True, TypeError]]
    )
    def test_exception(self, value, expected):
        with pytest.raises(expected):
            AttrList(value)


class Test_Value(object):
    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            [0, "0"],
            ["0", "0"],
            [1.1, "1.1"],
            ["test", "'test'"],
            ["te st", "'te st'"],
            ["I'm", '"I\'m"'],
            [None, "NULL"],
            [False, "'False'"],
        ],
    )
    def test_normal(self, value, expected):
        assert_query_item(Value(value), expected)

    @pytest.mark.parametrize(["value", "expected"], [[nan, "'nan'"], [inf, "'inf'"]])
    def test_abnormal(self, value, expected):
        assert_query_item(Value(value), expected)


class Test_Where(object):
    @pytest.mark.parametrize(
        ["key", "value", "operation", "expected"],
        [
            ["tkey", "tvalue", "=", "tkey = 'tvalue'"],
            ["key key", "tvalue", "!=", "[key key] != 'tvalue'"],
            ["%key+key", 100, "<", "[%key+key] < 100"],
            ["key.key", "555", ">", "[key.key] > 555"],
            ["tkey", None, "=", "tkey IS NULL"],
            ["tkey", None, "!=", "tkey IS NOT NULL"],
        ],
    )
    def test_normal(self, key, value, operation, expected):
        assert_query_item(Where(key, value, operation), expected)

    @pytest.mark.parametrize(
        ["key", "value", "operation", "expected"],
        [
            ["tkey", "tvalue", None, SqlSyntaxError],
            ["tkey", "tvalue", "INVALID_VALUE", SqlSyntaxError],
        ],
    )
    def test_exception(self, key, value, operation, expected):
        with pytest.raises(expected):
            "{}".format(Where(key, value, operation))


class Test_Select(object):
    @pytest.mark.parametrize(
        ["select", "table", "where", "extra", "expected"],
        [
            ["A", "B", None, None, "SELECT A FROM B"],
            ["A", "B B", None, None, "SELECT A FROM 'B B'"],
            ["A", "B-B", Where("C", 1), None, "SELECT A FROM [B-B] WHERE C = 1"],
            [
                "A",
                "B-B",
                Where("C", 1, cmp_operator=">"),
                "ORDER BY D",
                "SELECT A FROM [B-B] WHERE C > 1 ORDER BY D",
            ],
        ],
    )
    def test_normal(self, select, table, where, extra, expected):
        assert_query_item(Select(select, table, where, extra), expected)

    @pytest.mark.parametrize(
        ["select", "table", "where", "extra", "expected"],
        [
            ["A", None, None, None, NameValidationError],
            ["", "B", None, None, ValueError],
            [None, None, None, None, NameValidationError],
            [None, "B", None, None, ValueError],
            [nan, None, None, None, NameValidationError],
            [nan, nan, None, None, TypeError],
            [nan, nan, nan, None, TypeError],
            [nan, nan, nan, nan, TypeError],
        ],
    )
    def test_exception(self, select, table, where, extra, expected):
        with pytest.raises(expected):
            "{}".format(Select(select, table, where, extra))


class Test_And(object):
    @pytest.mark.parametrize(
        ["where_list", "expected"],
        [[[Where("hoge", "abc"), Where("bar", 100, ">")], "hoge = 'abc' AND bar > 100"], [[], ""]],
    )
    def test_normal(self, where_list, expected):
        assert_query_item(And(where_list), expected)


class Test_make_index_name(object):
    SANITIZE_CHAR_LIST = [
        ":",
        "*",
        "?",
        "|",
        "%",
        "/",
        "(",
        ")",
        "[",
        "]",
        "<",
        ">",
        ".",
        ";",
        "'",
        '"',
        "!",
        "\\",
        "#",
        " ",
        "-",
        "+",
        "=",
        " ",
        "\t",
        "\n",
        "\r",
        "\f",
        "\v",
    ]

    @pytest.mark.parametrize(
        ["value", "expected"],
        [["AAA{:s}".format(re.escape(c)), "AAA"] for c in SANITIZE_CHAR_LIST]
        + [["{:s}BBB".format(re.escape(c)), "BBB"] for c in SANITIZE_CHAR_LIST]
        + [
            [
                "%a/b(c)d[e]f<g>h.i;j'k!l\\m#n _o-p+q=r\nstrvwxyz" + os.linesep,
                "abcdefghijklmn_opqrstrvwxyz",
            ]
        ],
    )
    def test_normal(self, value, expected):
        flags = 0
        if six.PY3:
            flags = re.ASCII

        assert re.search("\w+_index_\w{4}", make_index_name(value, value), flags)

    def test_normal_symbol(self):
        assert make_index_name("table", "ABCD>8.5") != make_index_name("table", "ABCD<8.5")

    def test_normal_unicode(self):
        assert make_index_name("テーブル", "ほげ")
