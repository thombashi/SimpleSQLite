'''
@author: Tsuyoshi Hombashi

:required:
    https://pypi.python.org/pypi/pytest
'''

import itertools
import os
import re
import sqlite3

from collections import namedtuple
import dataproperty
import pytest
from six.moves import range

from simplesqlite import *


nan = float("nan")
inf = float("inf")
TEST_TABLE_NAME = "test_table"
TEST_DB_NAME = "test_db"
NOT_EXIT_FILE_PATH = "/not/existing/file/__path__"

NamedTuple = namedtuple("NamedTuple", "attr_a attr_b")
NamedTupleEx = namedtuple("NamedTupleEx", "attr_a attr_b attr_c")


class Test_SqlQuery_sanitize:
    SANITIZE_CHAR_LIST = [
        "%", "/", "(", ")", "[", "]", "<", ">", ".", ";",
        "'", "!", "\\", "#", " ", "-", "+", "=", "\n"
    ]

    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            ["AAA%s" % (re.escape(c)), "AAA"] for c in SANITIZE_CHAR_LIST
        ] + [
            ["%sBBB" % (re.escape(c)), "BBB"] for c in SANITIZE_CHAR_LIST
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
        ["test", "AVG", "AVG(test)"],
        ["attr_a", 2, "attr_a"],
        ["attr_a", True, "attr_a"],
    ] + [
        ["te%sst" % (re.escape(c)), None, "[te%sst]" % (re.escape(c))]
        for c in [
            "%", "(", ")", ".", " ", "-", "+", "#"
        ] + [str(n) for n in range(10)]
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
        ["table", "insert_tuple", "is_isert_many", "expected"], [
            [
                "A", ["B"], False,
                "INSERT INTO A VALUES ('B')"
            ],
            [
                "A", ["AAAA", 2], False,
                "INSERT INTO A VALUES ('AAAA',2)"
            ],
            [
                "A", ["B"], True,
                "INSERT INTO A VALUES (?)"
            ],
            [
                "A", ["B", "C"], True,
                "INSERT INTO A VALUES (?,?)"
            ],
        ])
    def test_normal(self, table, insert_tuple, is_isert_many, expected):
        assert SqlQuery.make_insert(
            table, insert_tuple, is_isert_many) == expected

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

    """
    @pytest.mark.parametrize(
        ["select", "table", "where", "extra", "expected"], [
            ["A", None, None, None, ValueError],
            [None, None, None, None, ValueError],
            [None, "B", None, None, TypeError],
            [nan, None, None, None, ValueError],
            [nan, nan, None, None, ValueError],
            [nan, nan, nan, None, ValueError],
            [nan, nan, nan, nan, ValueError],
        ])
    def test_exception(self, select, table, where, extra, expected):
        with pytest.raises(expected):
            SqlQuery.make_select(select, table, where, extra)
    """


class Test_SqlQuery_make_where:

    @pytest.mark.parametrize(["key", "value", "operation", "expected"], [
        ["key", "value", "=", "key = 'value'"],
        ["key key", "value", "!=", "[key key] != 'value'"],
        ["%key+key", 100, "<", "[%key+key] < 100"],
        ["key.key", "555", ">", "[key.key] > 555"],

        ["key", None, "!=", "key != NULL"],
    ])
    def test_normal(self, key, value, operation, expected):
        assert SqlQuery.make_where(key, value, operation) == expected

    @pytest.mark.parametrize(["key", "value", "operation", "expected"], [
        ["key", "value", None, ValueError],
        ["key", "value", "INVALID_VALUE", ValueError],
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


@pytest.fixture
def con(tmpdir):
    p = tmpdir.join("tmp.db")
    con = SimpleSQLite(str(p), "w")

    con.create_table_with_data(
        table_name=TEST_TABLE_NAME,
        attribute_name_list=["attr_a", "attr_b"],
        data_matrix=[
            [1, 2],
            [3, 4],
        ])

    return con


@pytest.fixture
def con_mix(tmpdir):
    p = tmpdir.join("tmp.db")
    con = SimpleSQLite(str(p), "w")

    con.create_table_with_data(
        table_name=TEST_TABLE_NAME,
        attribute_name_list=["attr_i", "attr_f", "attr_s"],
        data_matrix=[
            [1, 2.2, "aa"],
            [3, 4.4, "bb"],
        ])

    return con


@pytest.fixture
def con_ro(tmpdir):
    p = tmpdir.join("tmp.db")
    con = SimpleSQLite(str(p), "w")

    con.create_table_with_data(
        table_name=TEST_TABLE_NAME,
        attribute_name_list=["attr_a", "attr_b"],
        data_matrix=[
            [1, 2],
            [3, 4],
        ])
    con.close()
    con.connect(str(p), "r")

    return con


@pytest.fixture
def con_profile(tmpdir):
    p = tmpdir.join("tmp_profile.db")
    con = SimpleSQLite(str(p), "w", profile=True)

    con.create_table_with_data(
        table_name=TEST_TABLE_NAME,
        attribute_name_list=["attr_a", "attr_b"],
        data_matrix=[
            [1, 2],
            [3, 4],
        ])

    return con


@pytest.fixture
def con_null(tmpdir):
    p = tmpdir.join("tmp.db")
    con = SimpleSQLite(str(p), "w")
    con.close()

    return con


class Test_SimpleSQLite_init:

    @pytest.mark.parametrize(["mode"], [
        ["w"],
        ["a"],
    ])
    def test_normal(self, tmpdir, mode):
        p = tmpdir.join("not_exist.db")
        db_path = str(p)
        con = SimpleSQLite(db_path, mode)
        assert con.database_path == db_path
        assert con.connection is not None

    @pytest.mark.parametrize(["value", "mode", "expected"], [
        [NOT_EXIT_FILE_PATH, "r", IOError],
        [NOT_EXIT_FILE_PATH, "w", sqlite3.OperationalError],
        [NOT_EXIT_FILE_PATH, "a", sqlite3.OperationalError],

        [NOT_EXIT_FILE_PATH, None, TypeError],
        [NOT_EXIT_FILE_PATH, inf, TypeError],
        [NOT_EXIT_FILE_PATH, "", ValueError],
        [NOT_EXIT_FILE_PATH, "b", ValueError],
    ] + [
        arg_list
        for arg_list in itertools.product(
            [None, nan, ""], ["r", "w", "a"], [ValueError])
    ])
    def test_exception_1(self, value, mode, expected):
        with pytest.raises(expected):
            SimpleSQLite(value, mode)

    @pytest.mark.parametrize(["mode", "expected"], [
        ["r", IOError],
    ])
    def test_exception_2(self, tmpdir, mode, expected):
        p = tmpdir.join("not_exist.db")

        with pytest.raises(expected):
            SimpleSQLite(str(p), mode)


class Test_SimpleSQLite_is_connected:

    def test_normal(self, con):
        assert con.is_connected()

    def test_null(self, con_null):
        assert not con_null.is_connected()


class Test_SimpleSQLite_check_connection:

    def test_normal(self, con):
        con.check_connection()

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.check_connection()


class Test_SimpleSQLite_select:

    def test_smoke(self, con):
        result = con.select(select="*", table=TEST_TABLE_NAME)
        assert result is not None

    @pytest.mark.parametrize(["attr", "table_name", "expected"], [
        ["not_exist_attr", TEST_TABLE_NAME, sqlite3.OperationalError],
        ["", TEST_TABLE_NAME, ValueError],
        [None, TEST_TABLE_NAME, ValueError],
        ["attr_a", "not_exist_table", sqlite3.OperationalError],
        ["attr_a", "", ValueError],
        ["attr_a", None, ValueError],
        ["", "", ValueError],
        ["", None, ValueError],
        [None, None, ValueError],
        [None, "", ValueError],
    ])
    def test_exception(self, con, attr, table_name, expected):
        with pytest.raises(expected):
            con.select(select=attr, table=table_name)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.select(select="*", table=TEST_TABLE_NAME)


class Test_SimpleSQLite_insert:

    @pytest.mark.parametrize(["value", "expeted"], [
        [[5, 6], (5, 6)],
        [(5, 6), (5, 6)],
        [
            {"attr_a": 5, "attr_b": 6},
            (5, 6)
        ],
        [
            {"attr_a": 5, "attr_b": 6, "not_exist_attr": 100},
            (5, 6)
        ],
        [{"attr_a": 5}, (5, None)],
        [{"attr_b": 6}, (None, 6)],
        [{}, (None, None)],
        [NamedTuple(5, 6), (5, 6)],
        [NamedTuple(5, None), (5, None)],
        [NamedTuple(None, 6), (None, 6)],
        [NamedTuple(None, None), (None, None)],
        [NamedTupleEx(5, 6, 7), (5, 6)]
    ])
    def test_normal(self, con, value, expeted):
        assert con.get_value(select="COUNT(*)", table=TEST_TABLE_NAME) == 2
        con.insert(TEST_TABLE_NAME, insert_record=value)
        assert con.get_value(select="COUNT(*)", table=TEST_TABLE_NAME) == 3
        result = con.select(select="*", table=TEST_TABLE_NAME)
        result_tuple = result.fetchall()[2]
        assert result_tuple == expeted

    @pytest.mark.parametrize(["value", "expeted"], [
        [[5, 6.6, "c"], (5, 6.6, "c")],
    ])
    def test_mix(self, con_mix, value, expeted):
        assert con_mix.get_value(select="COUNT(*)", table=TEST_TABLE_NAME) == 2
        con_mix.insert(TEST_TABLE_NAME, insert_record=value)
        assert con_mix.get_value(select="COUNT(*)", table=TEST_TABLE_NAME) == 3
        result = con_mix.select(select="*", table=TEST_TABLE_NAME)
        result_tuple = result.fetchall()[2]
        assert result_tuple == expeted

    def test_read_only(self, con_ro):
        with pytest.raises(IOError):
            con_ro.insert(
                TEST_TABLE_NAME, insert_record=[5, 6])

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.insert(
                TEST_TABLE_NAME, insert_record=[5, 6])


class Test_SimpleSQLite_insert_many:

    @pytest.mark.parametrize(["table_name", "value"], [
        [
            TEST_TABLE_NAME,
            [
                [7, 8],
                [9, 10],
                [11, 12],
            ],
        ],
        [
            TEST_TABLE_NAME,
            [
                {"attr_a": 7, "attr_b": 8},
                {"attr_a": 9, "attr_b": 10},
                {"attr_a": 11, "attr_b": 12},
            ],
        ],
        [
            TEST_TABLE_NAME,
            [
                NamedTuple(7, 8),
                NamedTuple(9, 10),
                NamedTuple(11, 12),
            ],
        ],
        [
            TEST_TABLE_NAME,
            [
                [7, 8],
                {"attr_a": 9, "attr_b": 10},
                NamedTuple(11, 12),
            ],
        ],
    ])
    def test_normal(self, con, table_name, value):
        expected = [
            (7, 8),
            (9, 10),
            (11, 12),
        ]

        assert con.get_value(select="COUNT(*)", table=TEST_TABLE_NAME) == 2
        con.insert_many(TEST_TABLE_NAME, value)
        assert con.get_value(select="COUNT(*)", table=TEST_TABLE_NAME) == 5
        result = con.select(select="*", table=TEST_TABLE_NAME)
        result_tuple = result.fetchall()[2:]
        assert result_tuple == expected

    @pytest.mark.parametrize(["table_name", "value"], [
        [TEST_TABLE_NAME, []],
        [TEST_TABLE_NAME, None],
    ])
    def test_empty(self, con, table_name, value):
        con.insert_many(TEST_TABLE_NAME, value)

    @pytest.mark.parametrize(["table_name", "value", "expected"], [
        [None, None, ValueError],
        [None, [], ValueError],
        [TEST_TABLE_NAME, [None], ValueError],
    ])
    def test_exception(self, con, table_name, value, expected):
        with pytest.raises(expected):
            con.insert_many(table_name, value)

    def test_read_only(self, con_ro):
        with pytest.raises(IOError):
            con_ro.insert(
                TEST_TABLE_NAME, [])

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.insert_many(
                TEST_TABLE_NAME, [])


class Test_SimpleSQLite_update:

    def test_normal(self, con):
        table_name = TEST_TABLE_NAME
        where = SqlQuery.make_where("attr_b", 2)
        con.update(table=table_name, set_query="attr_a = 100", where=where)
        assert con.get_value(
            select="attr_a", table=table_name, where=where) == 100

    @pytest.mark.parametrize(["table_name", "set_query", "expected"], [
        [TEST_TABLE_NAME, "", ValueError],
        [TEST_TABLE_NAME, None, ValueError],
        ["not_exist_table", "attr_a = 1", sqlite3.OperationalError],
        ["", "attr_a = 1", ValueError],
        [None, "attr_a = 1", ValueError],
        ["", "", ValueError],
        ["", None, ValueError],
        [None, None, ValueError],
        [None, "", ValueError],
    ])
    def test_exception(self, con, table_name, set_query, expected):
        with pytest.raises(expected):
            con.update(table=table_name, set_query=set_query)

    def test_read_only(self, con_ro):
        with pytest.raises(IOError):
            con_ro.update(
                table=TEST_TABLE_NAME, set_query="attr_a = 100")

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.update(table=TEST_TABLE_NAME, set_query="hoge")


class Test_SimpleSQLite_get_total_changes:

    def test_smoke(self, con):
        assert con.get_total_changes() > 0

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.get_total_changes()


class Test_SimpleSQLite_get_table_name_list:

    def test_normal(self, con):
        expected = set([
            SimpleSQLite.TableConfiguration.TABLE_NAME, TEST_TABLE_NAME
        ])

        assert set(con.get_table_name_list()) == expected

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.get_table_name_list()


class Test_SimpleSQLite_get_attribute_name_list:

    @pytest.mark.parametrize(["value", "expected"], [
        [
            TEST_TABLE_NAME,
            ["attr_a", "attr_b"],
        ],
    ])
    def test_normal(self, con,  value, expected):
        assert con.get_attribute_name_list(value) == expected

    @pytest.mark.parametrize(["value", "expected"], [
        ["not_exist_table", TableNotFoundError],
        [None, TableNotFoundError],
    ])
    def test_null(self, con, value, expected):
        with pytest.raises(expected):
            con.get_attribute_name_list(value)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.get_attribute_name_list("not_exist_table")


class Test_SimpleSQLite_get_attribute_type_list:

    @pytest.mark.parametrize(["value", "expected"], [
        [
            TEST_TABLE_NAME,
            ("integer", "integer"),
        ],
    ])
    def test_normal(self, con,  value, expected):
        assert con.get_attribute_type_list(value) == expected

    @pytest.mark.parametrize(["value", "expected"], [
        ["not_exist_table", TableNotFoundError],
        [None, TableNotFoundError],
    ])
    def test_null(self, con, value, expected):
        with pytest.raises(expected):
            con.get_attribute_type_list(value)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.get_attribute_type_list(TEST_TABLE_NAME)


class Test_SimpleSQLite_has_table:

    @pytest.mark.parametrize(["value", "expected"], [
        [TEST_TABLE_NAME, True],
        ["not_exist_table", False],
        ["", False],
        [None, False],
    ])
    def test_normal(self, con, value, expected):
        assert con.has_table(value) == expected

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.has_table(TEST_TABLE_NAME)


class Test_SimpleSQLite_has_attribute:

    @pytest.mark.parametrize(["table", "attr", "expected"], [
        [TEST_TABLE_NAME, "attr_a", True],
        [TEST_TABLE_NAME, "not_exist_attr", False],
        [TEST_TABLE_NAME, "", False],
        [TEST_TABLE_NAME, None, False],
    ])
    def test_normal(self, con, table, attr, expected):
        assert con.has_attribute(table, attr) == expected

    @pytest.mark.parametrize(["value", "attr", "expected"], [
        ["not_exist_table", "attr_a", TableNotFoundError],
        [None, "attr_a", ValueError],
        ["", "attr_a", ValueError],
    ])
    def test_exception(self, con, value, attr, expected):
        with pytest.raises(expected):
            con.has_attribute(value, attr)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.has_attribute(TEST_TABLE_NAME, "attr")


class Test_SimpleSQLite_has_attribute_list:

    @pytest.mark.parametrize(["table", "attr", "expected"], [
        [TEST_TABLE_NAME, ["attr_a"], True],
        [TEST_TABLE_NAME, ["attr_a", "attr_b"], True],
        [TEST_TABLE_NAME, ["attr_a", "attr_b", "not_exist_attr"], False],
        [TEST_TABLE_NAME, ["not_exist_attr"], False],
        [TEST_TABLE_NAME, [], False],
        [TEST_TABLE_NAME, None, False],
    ])
    def test_normal(self, con, table, attr, expected):
        assert con.has_attribute_list(table, attr) == expected

    @pytest.mark.parametrize(["value", "attr", "expected"], [
        ["not_exist_table", ["attr_a"], TableNotFoundError],
        [None, ["attr_a"], ValueError],
        ["", ["attr_a"], ValueError],
    ])
    def test_exception(self, con, value, attr, expected):
        with pytest.raises(expected):
            con.has_attribute_list(value, attr)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.has_attribute_list(TEST_TABLE_NAME, "attr")


class Test_SimpleSQLite_get_profile:

    def test_normal(self, con):
        attribute_name_list, profile_list = con.get_profile()
        assert dataproperty.is_empty_list_or_tuple(attribute_name_list)
        assert dataproperty.is_empty_list_or_tuple(profile_list)

    def test_normal_profile(self, con_profile):
        attribute_name_list, profile_list = con_profile.get_profile()
        assert dataproperty.is_not_empty_list_or_tuple(attribute_name_list)
        assert dataproperty.is_not_empty_list_or_tuple(profile_list)


class Test_SimpleSQLite_verify_table_existence:

    def test_normal(self, con):
        con.verify_table_existence(TEST_TABLE_NAME)

    def test_exception(self, con):
        with pytest.raises(TableNotFoundError):
            con.verify_table_existence("not_exist_table")

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.verify_table_existence(TEST_TABLE_NAME)


class Test_SimpleSQLite_verify_attribute_existence:

    @pytest.mark.parametrize(["table", "attr", "expected"], [
        [TEST_TABLE_NAME, "not_exist_attr", AttributeNotFoundError],
        ["not_exist_table", "attr_a", TableNotFoundError],
        [None, "attr_a", ValueError],
        ["", "attr_a", ValueError],
    ])
    def test_normal(self, con, table, attr, expected):
        with pytest.raises(expected):
            con.verify_attribute_existence(table, attr)


class Test_SimpleSQLite_drop_table:

    def test_normal(self, con):
        attr_description_list = [
            "'%s' %s" % ("attr_name", "TEXT")
        ]

        table_name = "new_table"

        assert not con.has_table(table_name)

        con.create_table(table_name, attr_description_list)
        assert con.has_table(table_name)

        con.drop_table(table_name)
        assert not con.has_table(table_name)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.drop_table(TEST_TABLE_NAME)


class Test_SimpleSQLite_create_table_with_data:

    @pytest.mark.parametrize(["data_matrix"], [
        [
            [
                [1, 4,      "a"],
                [2, 2.1,    "bb"],
                [3, 120.9,  "ccc"],
            ],
        ],
        [
            [
                {"attr_a": 1, "attr_b": 4,      "attr_c": "a"},
                {"attr_a": 2, "attr_b": 2.1,    "attr_c": "bb"},
                {"attr_a": 3, "attr_b": 120.9,  "attr_c": "ccc"},
            ],
        ],
    ])
    def test_normal(self, tmpdir, data_matrix):
        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")
        table_name = TEST_TABLE_NAME
        attribute_name_list = ["attr_a", "attr_b", "attr_c"]
        index_attribute_list = ["attr_a"]

        con.create_table_with_data(
            table_name, attribute_name_list, data_matrix, index_attribute_list)
        con.commit()

        # check data ---
        result = con.select(select="*", table=table_name)
        result_matrix = result.fetchall()
        assert len(result_matrix) == 3

        # check table config ---
        expected = [
            (table_name, 'attr_a', 'INTEGER', 1),
            (table_name, 'attr_b', 'REAL', 0),
            (table_name, 'attr_c', 'TEXT', 0),
        ]

        result = con.select(
            select="*", table=SimpleSQLite.TableConfiguration.TABLE_NAME)
        result_matrix = result.fetchall()
        assert result_matrix == expected

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.create_table_with_data(
                TEST_TABLE_NAME, [], [])


class Test_SimpleSQLite_rollback:

    def test_normal(self, con):
        con.rollback()

    def test_null(self, con_null):
        con_null.rollback()


class Test_SimpleSQLite_commit:

    def test_normal(self, con):
        con.commit()

    def test_null(self, con_null):
        con_null.commit()


class Test_SimpleSQLite_close:

    def test_close(self, con):
        con.close()

    def test_null(self, con_null):
        con_null.close()


class Test_connect_sqlite_db_mem:

    def test_normal(self):
        assert connect_sqlite_db_mem() is not None
