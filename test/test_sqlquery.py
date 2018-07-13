# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import unicode_literals

import pytest
from simplesqlite.query import Where
from simplesqlite.sqlquery import SqlQuery


nan = float("nan")
inf = float("inf")


class Test_SqlQuery_make_insert(object):
    @pytest.mark.parametrize(
        ["table", "insert_tuple", "expected"],
        [
            ["A", ["B"], "INSERT INTO A VALUES (?)"],
            ["A", ["B", "C"], "INSERT INTO A VALUES (?,?)"],
            ["A", ["AAAA", 2, 0.1], "INSERT INTO A VALUES (?,?,?)"],
        ],
    )
    def test_normal(self, table, insert_tuple, expected):
        assert SqlQuery.make_insert(table, insert_tuple) == expected

    @pytest.mark.parametrize(
        ["table", "insert_tuple", "is_isert_many", "expected"],
        [
            ["", [], False, ValueError],
            ["", None, True, ValueError],
            ["", ["B"], False, ValueError],
            ["A", [], True, ValueError],
            ["A", None, False, ValueError],
            [None, None, True, ValueError],
            [None, ["B"], False, ValueError],
            [None, [], True, ValueError],
        ],
    )
    def test_exception(self, table, insert_tuple, is_isert_many, expected):
        with pytest.raises(expected):
            SqlQuery.make_insert(table, insert_tuple)


class Test_SqlQuery_make_update(object):
    @pytest.mark.parametrize(
        ["table", "set_query", "where", "expected"],
        [
            ["A", "B=1", None, "UPDATE A SET B=1"],
            ["A", "B=1", Where("C", 1, ">").to_query(), "UPDATE A SET B=1 WHERE C > 1"],
        ],
    )
    def test_normal(self, table, set_query, where, expected):
        assert SqlQuery.make_update(table, set_query, where) == expected

    @pytest.mark.parametrize(
        ["table", "set_query", "where", "expected"],
        [
            [None, "B=1", None, ValueError],
            ["", "B=1", None, ValueError],
            ["A", None, None, ValueError],
            ["A", "", None, ValueError],
        ],
    )
    def test_exception(self, table, set_query, where, expected):
        with pytest.raises(expected):
            SqlQuery.make_update(table, set_query, where)


class Test_SqlQuery_make_where_in(object):
    @pytest.mark.parametrize(
        ["key", "value", "expected"], [["key", ["attr_a", "attr_b"], "key IN ('attr_a', 'attr_b')"]]
    )
    def test_normal(self, key, value, expected):
        assert SqlQuery.make_where_in(key, value) == expected

    @pytest.mark.parametrize(
        ["key", "value", "expected"],
        [
            ["key", None, TypeError],
            ["key", 1, TypeError],
            [None, ["attr_a", "attr_b"], TypeError],
            [None, None, TypeError],
        ],
    )
    def test_exception(self, key, value, expected):
        with pytest.raises(expected):
            SqlQuery.make_where_in(key, value)


class Test_SqlQuery_make_where_not_in(object):
    @pytest.mark.parametrize(
        ["key", "value", "expected"],
        [["key", ["attr_a", "attr_b"], "key NOT IN ('attr_a', 'attr_b')"]],
    )
    def test_normal(self, key, value, expected):
        assert SqlQuery.make_where_not_in(key, value) == expected

    @pytest.mark.parametrize(
        ["key", "value", "expected"],
        [
            ["key", None, TypeError],
            ["key", 1, TypeError],
            [None, ["attr_a", "attr_b"], TypeError],
            [None, None, TypeError],
        ],
    )
    def test_exception(self, key, value, expected):
        with pytest.raises(expected):
            SqlQuery.make_where_not_in(key, value)
