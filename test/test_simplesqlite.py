"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

import datetime
import itertools
import json
from collections import OrderedDict, namedtuple
from decimal import Decimal

import pytest
import typepy
from tabledata import TableData

from simplesqlite import (
    AttributeNotFoundError,
    DatabaseError,
    NameValidationError,
    NullDatabaseConnectionError,
    OperationalError,
    SimpleSQLite,
    TableNotFoundError,
    connect_memdb,
)
from simplesqlite.query import Attr, AttrList, Where

from ._common import print_test_result
from .fixture import (
    TEST_TABLE_NAME,
    con,
    con_empty,
    con_index,
    con_mix,
    con_null,
    con_profile,
    con_ro,
)


nan = float("nan")
inf = float("inf")
TEST_DB_NAME = "test_db"
NOT_EXIT_FILE_PATH = "/not/existing/file/__path__"

NamedTuple = namedtuple("NamedTuple", "attr_a attr_b")
NamedTupleEx = namedtuple("NamedTupleEx", "attr_a attr_b attr_c")


class Test_SimpleSQLite_init:
    @pytest.mark.parametrize(["mode"], [["w"], ["a"]])
    def test_normal_path(self, tmpdir, mode):
        p = tmpdir.join("test.sqlite3")
        db_path = str(p)
        con = SimpleSQLite(db_path, mode)

        assert con.database_path == db_path
        assert con.connection is not None

    @pytest.mark.parametrize(["mode"], [["w"], ["a"]])
    def test_normal_con(self, mode):
        con = SimpleSQLite(connect_memdb().connection, mode)
        assert con.database_path is None
        assert con.connection

        con = SimpleSQLite(connect_memdb(), mode)
        assert con.database_path
        assert con.connection

    @pytest.mark.parametrize(
        ["value", "mode", "expected"],
        [
            [NOT_EXIT_FILE_PATH, "r", IOError],
            [NOT_EXIT_FILE_PATH, "w", OperationalError],
            [NOT_EXIT_FILE_PATH, "a", OperationalError],
            [NOT_EXIT_FILE_PATH, None, TypeError],
            [NOT_EXIT_FILE_PATH, inf, TypeError],
            [NOT_EXIT_FILE_PATH, "", ValueError],
            [NOT_EXIT_FILE_PATH, "b", ValueError],
            ["", "r", ValueError],
        ]
        + [args for args in itertools.product([None, nan], ["r", "w", "a"], [TypeError])],
    )
    def test_exception_invalid_arg(self, value, mode, expected):
        with pytest.raises(expected):
            SimpleSQLite(value, mode).connection

    @pytest.mark.parametrize(
        ["mode", "expected"], [["r", DatabaseError], ["w", DatabaseError], ["a", DatabaseError]]
    )
    def test_exception_invalid_file(self, tmpdir, mode, expected):
        p = tmpdir.join("testdata.txt")

        with open(str(p), "w") as f:
            f.write("dummy data")

        with pytest.raises(expected):
            SimpleSQLite(str(p), mode).connection


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
        result = con.select(select="*", table_name=TEST_TABLE_NAME)
        assert result is not None

    @pytest.mark.parametrize(
        ["attr", "table_name", "expected"],
        [
            ["not_exist_attr", TEST_TABLE_NAME, OperationalError],
            ["", TEST_TABLE_NAME, ValueError],
            [None, TEST_TABLE_NAME, ValueError],
            ["attr_a", "not_exist_table", TableNotFoundError],
            ["attr_a", "", ValueError],
            ["attr_a", None, ValueError],
            ["", "", ValueError],
            ["", None, ValueError],
            [None, None, ValueError],
            [None, "", ValueError],
        ],
    )
    def test_exception(self, con, attr, table_name, expected):
        with pytest.raises(expected):
            con.select(select=attr, table_name=table_name)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.select(select="*", table_name=TEST_TABLE_NAME)


class Test_SimpleSQLite_select_as_dict:
    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            [
                TableData(
                    "json1",
                    ["attr_a", "attr_b", "attr_c"],
                    [
                        {"attr_a": 1, "attr_b": 4, "attr_c": "a"},
                        {"attr_a": 2, "attr_b": 2.1, "attr_c": "bb"},
                        {"attr_a": 3, "attr_b": 120.9, "attr_c": "ccc"},
                    ],
                ),
                [
                    OrderedDict([("attr_a", 1), ("attr_b", 4), ("attr_c", "a")]),
                    OrderedDict([("attr_a", 2), ("attr_b", Decimal("2.1")), ("attr_c", "bb")]),
                    OrderedDict([("attr_a", 3), ("attr_b", Decimal("120.9")), ("attr_c", "ccc")]),
                ],
            ],
            [
                TableData("123num", ["123abc", "999"], [{"123abc": 1, "999": 4},],),
                [OrderedDict([("123abc", 1), ("999", 4)]),],
            ],
            [
                TableData("num123num", ["abc123abc", "999"], [{"abc123abc": 1, "999": 4},],),
                [OrderedDict([("abc123abc", 1), ("999", 4)]),],
            ],
        ],
    )
    def test_normal(self, tmpdir, value, expected):
        p_db = tmpdir.join("tmp.db")

        con = SimpleSQLite(str(p_db), "w")
        con.create_table_from_tabledata(value)

        assert con.select_as_dict(table_name=value.table_name) == expected


class Test_SimpleSQLite_dump:
    def test_normal(self, con, tmpdir):
        dump_path = str(tmpdir.join("dump.db"))
        con.dump(dump_path)
        con_dump = SimpleSQLite(dump_path, "r")

        assert con.fetch_num_records(TEST_TABLE_NAME) == con_dump.fetch_num_records(TEST_TABLE_NAME)
        assert con.select_as_tabledata(TEST_TABLE_NAME) == con_dump.select_as_tabledata(
            TEST_TABLE_NAME
        )


class Test_SimpleSQLite_insert:
    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            [[5, 6], (5, 6)],
            [(5, 6), (5, 6)],
            [{"attr_a": 5, "attr_b": 6}, (5, 6)],
            [{"attr_a": 5, "attr_b": 6, "not_exist_attr": 100}, (5, 6)],
            [{"attr_a": 5}, (5, None)],
            [{"attr_b": 6}, (None, 6)],
            [{}, (None, None)],
            [NamedTuple(5, 6), (5, 6)],
            [NamedTuple(5, None), (5, None)],
            [NamedTuple(None, 6), (None, 6)],
            [NamedTuple(None, None), (None, None)],
            [NamedTupleEx(5, 6, 7), (5, 6)],
        ],
    )
    def test_normal(self, con, value, expected):
        assert con.fetch_num_records(TEST_TABLE_NAME) == 2
        con.insert(TEST_TABLE_NAME, record=value)
        assert con.fetch_num_records(TEST_TABLE_NAME) == 3
        result = con.select(select="*", table_name=TEST_TABLE_NAME)
        result_tuple = result.fetchall()[2]
        assert result_tuple == expected

    @pytest.mark.parametrize(["value", "expected"], [[[5, 6.6, "c"], (5, 6.6, "c")]])
    def test_mix(self, con_mix, value, expected):
        assert con_mix.fetch_num_records(TEST_TABLE_NAME) == 2
        con_mix.insert(TEST_TABLE_NAME, record=value)
        assert con_mix.fetch_num_records(TEST_TABLE_NAME) == 3
        result = con_mix.select(select="*", table_name=TEST_TABLE_NAME)
        result_tuple = result.fetchall()[2]
        assert result_tuple == expected

    def test_read_only(self, con_ro):
        with pytest.raises(IOError):
            con_ro.insert(TEST_TABLE_NAME, record=[5, 6])

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.insert(TEST_TABLE_NAME, record=[5, 6])


class Test_SimpleSQLite_insert_many:
    @pytest.mark.parametrize(
        ["table_name", "records"],
        [
            [TEST_TABLE_NAME, [[7, 8], [9, 10], [11, 12]]],
            [
                TEST_TABLE_NAME,
                [
                    {"attr_a": 7, "attr_b": 8},
                    {"attr_a": 9, "attr_b": 10},
                    {"attr_a": 11, "attr_b": 12},
                ],
            ],
            [TEST_TABLE_NAME, [NamedTuple(7, 8), NamedTuple(9, 10), NamedTuple(11, 12)]],
            [TEST_TABLE_NAME, [[7, 8], {"attr_a": 9, "attr_b": 10}, NamedTuple(11, 12)]],
        ],
    )
    def test_normal(self, con, table_name, records):
        expected = [(7, 8), (9, 10), (11, 12)]

        assert con.fetch_num_records(TEST_TABLE_NAME) == 2
        assert con.insert_many(TEST_TABLE_NAME, records) == len(records)
        assert con.fetch_num_records(TEST_TABLE_NAME) == 5
        result = con.select(select="*", table_name=TEST_TABLE_NAME)
        result_tuple = result.fetchall()[2:]
        assert result_tuple == expected

    @pytest.mark.parametrize(
        ["table_name", "value"], [[TEST_TABLE_NAME, []], [TEST_TABLE_NAME, None]]
    )
    def test_empty(self, con, table_name, value):
        assert con.insert_many(TEST_TABLE_NAME, value) == 0

    @pytest.mark.parametrize(
        ["table_name", "value", "expected"],
        [[None, None, ValueError], [None, [], ValueError], [TEST_TABLE_NAME, [None], ValueError]],
    )
    def test_exception(self, con, table_name, value, expected):
        with pytest.raises(expected):
            con.insert_many(table_name, value)

    def test_read_only(self, con_ro):
        with pytest.raises(IOError):
            con_ro.insert(TEST_TABLE_NAME, [])

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.insert_many(TEST_TABLE_NAME, [])


class Test_SimpleSQLite_update:
    def test_normal(self, con):
        table_name = TEST_TABLE_NAME
        where = Where("attr_b", 2)
        con.update(table_name=table_name, set_query="attr_a = 100", where=where)
        assert con.fetch_value(select="attr_a", table_name=table_name, where=where) == 100
        assert (
            con.fetch_value(select="attr_a", table_name=table_name, where=Where("attr_b", 4)) == 3
        )

    @pytest.mark.parametrize(
        ["table_name", "set_query", "expected"],
        [
            [TEST_TABLE_NAME, "", ValueError],
            [TEST_TABLE_NAME, None, ValueError],
            ["not_exist_table", "attr_a = 1", TableNotFoundError],
            ["", "attr_a = 1", ValueError],
            [None, "attr_a = 1", ValueError],
            ["", "", ValueError],
            ["", None, ValueError],
            [None, None, ValueError],
            [None, "", ValueError],
        ],
    )
    def test_exception(self, con, table_name, set_query, expected):
        with pytest.raises(expected):
            con.update(table_name=table_name, set_query=set_query)

    def test_read_only(self, con_ro):
        with pytest.raises(IOError):
            con_ro.update(table_name=TEST_TABLE_NAME, set_query="attr_a = 100")

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.update(table_name=TEST_TABLE_NAME, set_query="hoge")


class Test_SimpleSQLite_total_changes:
    def test_smoke(self, con):
        assert con.total_changes > 0

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.total_changes


class Test_SimpleSQLite_fetch_table_names:
    def test_normal(self, con):
        expected = {TEST_TABLE_NAME}

        assert set(con.fetch_table_names()) == expected

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.fetch_table_names()


class Test_SimpleSQLite_fetch_attr_names:
    @pytest.mark.parametrize(["value", "expected"], [[TEST_TABLE_NAME, ["attr_a", "attr_b"]]])
    def test_normal(self, con, value, expected):
        assert con.fetch_attr_names(value) == expected

    @pytest.mark.parametrize(
        ["value", "expected"],
        [["not_exist_table", TableNotFoundError], [None, NameValidationError]],
    )
    def test_null_table(self, con, value, expected):
        with pytest.raises(expected):
            con.fetch_attr_names(value)

    def test_null_con(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.fetch_attr_names("not_exist_table")


class Test_SimpleSQLite_has_table:
    @pytest.mark.parametrize(
        ["value", "expected"],
        [[TEST_TABLE_NAME, True], ["not_exist_table", False], ["", False], [None, False]],
    )
    def test_normal(self, con, value, expected):
        assert con.has_table(value) == expected

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.has_table(TEST_TABLE_NAME)


class Test_SimpleSQLite_has_attr:
    @pytest.mark.parametrize(
        ["table", "attr", "expected"],
        [
            [TEST_TABLE_NAME, "attr_a", True],
            [TEST_TABLE_NAME, "not_exist_attr", False],
            [TEST_TABLE_NAME, "", False],
            [TEST_TABLE_NAME, None, False],
        ],
    )
    def test_normal(self, con, table, attr, expected):
        assert con.has_attr(table, attr) == expected

    @pytest.mark.parametrize(
        ["value", "attr", "expected"],
        [
            ["not_exist_table", "attr_a", TableNotFoundError],
            [None, "attr_a", ValueError],
            ["", "attr_a", ValueError],
        ],
    )
    def test_exception(self, con, value, attr, expected):
        with pytest.raises(expected):
            con.has_attr(value, attr)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.has_attr(TEST_TABLE_NAME, "attr")


class Test_SimpleSQLite_has_attrs:
    @pytest.mark.parametrize(
        ["table", "attr", "expected"],
        [
            [TEST_TABLE_NAME, ["attr_a"], True],
            [TEST_TABLE_NAME, ["attr_a", "attr_b"], True],
            [TEST_TABLE_NAME, ["attr_a", "attr_b", "not_exist_attr"], False],
            [TEST_TABLE_NAME, ["not_exist_attr"], False],
            [TEST_TABLE_NAME, [], False],
            [TEST_TABLE_NAME, None, False],
        ],
    )
    def test_normal(self, con, table, attr, expected):
        assert con.has_attrs(table, attr) == expected

    @pytest.mark.parametrize(
        ["value", "attr", "expected"],
        [
            ["not_exist_table", ["attr_a"], TableNotFoundError],
            [None, ["attr_a"], ValueError],
            ["", ["attr_a"], ValueError],
        ],
    )
    def test_exception(self, con, value, attr, expected):
        with pytest.raises(expected):
            con.has_attrs(value, attr)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.has_attrs(TEST_TABLE_NAME, "attr")


class Test_SimpleSQLite_get_profile:
    def test_normal(self, con):
        profile_list = con.get_profile()
        assert typepy.is_empty_sequence(profile_list)

    def test_normal_profile(self, con_profile):
        profile_list = con_profile.get_profile()
        assert typepy.is_not_empty_sequence(profile_list)


class Test_SimpleSQLite_fetch_sqlite_master:
    def test_normal(self, con_index):
        expected = [
            {
                "tbl_name": "test_table",
                "sql": 'CREATE TABLE \'test_table\' ("attr_a" INTEGER, "attr_b" INTEGER)',
                "type": "table",
                "name": "test_table",
                "rootpage": 2,
            },
            {
                "tbl_name": "test_table",
                "sql": 'CREATE INDEX testtable_attra_index_3013 ON test_table("attr_a")',
                "type": "index",
                "name": "testtable_attra_index_3013",
                "rootpage": 3,
            },
        ]
        actual = con_index.fetch_sqlite_master()
        print_test_result(
            expected=json.dumps(expected, indent=4), actual=json.dumps(actual, indent=4)
        )

        assert expected == actual

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.fetch_sqlite_master()


class Test_SimpleSQLite_verify_table_existence:
    def test_normal(self, con):
        con.verify_table_existence(TEST_TABLE_NAME)

    def test_exception(self, con):
        with pytest.raises(TableNotFoundError):
            con.verify_table_existence("not_exist_table")

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.verify_table_existence(TEST_TABLE_NAME)


class Test_SimpleSQLite_verify_attr_existence:
    @pytest.mark.parametrize(
        ["table", "attr", "expected"],
        [
            [TEST_TABLE_NAME, "not_exist_attr", AttributeNotFoundError],
            ["not_exist_table", "attr_a", TableNotFoundError],
            [None, "attr_a", ValueError],
            ["", "attr_a", ValueError],
        ],
    )
    def test_normal(self, con, table, attr, expected):
        with pytest.raises(expected):
            con.verify_attr_existence(table, attr)


class Test_SimpleSQLite_drop_table:
    def test_normal(self, con):
        attr_descriptions = ["'{:s}' {:s}".format("attr_name", "TEXT")]
        table_name = "new_table"

        assert not con.has_table(table_name)

        con.create_table(table_name, attr_descriptions)
        assert con.has_table(table_name)

        con.drop_table(table_name)
        assert not con.has_table(table_name)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.drop_table(TEST_TABLE_NAME)


class Test_SimpleSQLite_create_table_from_data_matrix:
    DATATIME_DATA = datetime.datetime(2017, 1, 1, 0, 0, 0)

    @pytest.mark.parametrize(
        ["attr_names", "data_matrix", "index_attrs", "expected_attr"],
        [
            [
                ["attr_a", "attr_b", "attr_c"],
                [[1, 4, "a"], [2, 2.1, "bb"], [3, 120.9, "ccc"]],
                ["attr_a"],
                {'"attr_c"': "TEXT", '"attr_b"': "REAL", '"attr_a"': "INTEGER"},
            ],
            [
                ["attr_a", "attr_b", "attr_c"],
                [
                    {"attr_a": 1, "attr_b": 4, "attr_c": "a"},
                    {"attr_a": 2, "attr_b": 2.1, "attr_c": "bb"},
                    {"attr_a": 3, "attr_b": 120.9, "attr_c": "ccc"},
                ],
                ["not_exist_attr_0", "attr_a", "attr_b", "attr_c", "not_exist_attr_1"],
                {'"attr_c"': "TEXT", '"attr_b"': "REAL", '"attr_a"': "INTEGER"},
            ],
            [
                [
                    "attr'a",
                    'attr"b',
                    "attr'c[%]",
                    "attr($)",
                    "attr inf",
                    "attr nan",
                    "attr-f",
                    "attr dt",
                    "aa,bb",
                    "sp a ce",
                ],
                [
                    [1, 4, "a", None, inf, nan, 0, DATATIME_DATA, ",,,", "a  "],
                    [2, 2.1, "bb", None, inf, nan, inf, DATATIME_DATA, "!!!", "  b"],
                    [2, 2.1, "bb", None, inf, nan, nan, DATATIME_DATA, "vvv", " c "],
                ],
                [
                    "attr'a",
                    'attr"b',
                    "attr'c[%]",
                    "attr($)",
                    "attr inf",
                    "attr nan",
                    "attr-f",
                    "attr dt",
                    "aa,bb",
                    "sp a ce",
                ],
                {
                    '"attr_a"': "INTEGER",
                    '"attr_b"': "REAL",
                    '"attr_c[%]"': "TEXT",
                    "[attr($)]": "TEXT",
                    "[attr inf]": "TEXT",
                    "[attr nan]": "TEXT",
                    "[attr-f]": "REAL",
                    "[attr dt]": "TEXT",
                    '"aa_bb"': "TEXT",
                    "[sp a ce]": "TEXT",
                },
            ],
            [
                ["index", "No", "Player_last_name", "Age", "Team"],
                [
                    [1, 55, "D Sam", 31, "Raven"],
                    [2, 36, "J Ifdgg", 30, "Raven"],
                    [3, 91, "K Wedfb", 28, "Raven"],
                ],
                [],
                {
                    "Age": "INTEGER",
                    "No": "INTEGER",
                    "Team": "TEXT",
                    '"Player_last_name"': "TEXT",
                    '"index"': "INTEGER",
                },
            ],
            [
                ["姓", "名", "生年月日", "郵便番号", "住所", "電話番号"],
                [
                    ["山田", "太郎", "2001/1/1", "100-0002", "東京都千代田区皇居外苑", "03-1234-5678"],
                    ["山田", "次郎", "2001/1/2", "251-0036", "神奈川県藤沢市江の島１丁目", "03-9999-9999"],
                    ["山田", "次郎", "2001/1/2", "251-0036", "神奈川県藤沢市江の島１丁目", "03-9999-9999"],
                ],
                [],
                {
                    "姓": "TEXT",
                    "名": "TEXT",
                    "生年月日": "TEXT",
                    "郵便番号": "TEXT",
                    "住所": "TEXT",
                    "電話番号": "TEXT",
                },
            ],
        ],
    )
    def test_normal(self, tmpdir, attr_names, data_matrix, index_attrs, expected_attr):
        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")
        table_name = TEST_TABLE_NAME

        con.create_table_from_data_matrix(
            table_name, attr_names, data_matrix, primary_key=None, index_attrs=index_attrs
        )

        # check data ---
        result = con.select(select=AttrList(attr_names), table_name=table_name)
        result_matrix = result.fetchall()
        assert len(result_matrix) == 3

        print_test_result(expected=expected_attr, actual=con.fetch_attr_type(table_name))
        assert con.fetch_attr_type(table_name) == expected_attr

    @pytest.mark.parametrize(
        ["table_name", "attr_names", "data_matrix", "expected"],
        [[TEST_TABLE_NAME, ["", None], [["a", 1], ["bb", 2]], ["A", "B"]]],
    )
    def test_normal_empty_header(self, tmpdir, table_name, attr_names, data_matrix, expected):
        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")

        con.create_table_from_data_matrix(table_name, attr_names, data_matrix)

        assert con.fetch_attr_names(table_name) == expected

    @pytest.mark.parametrize(
        ["table_name", "attr_names", "data_matrix", "expected"],
        [[TEST_TABLE_NAME, ["AA", "BB"], [["a", 1], ["bb", 2]], ["AA", "BB"]]],
    )
    def test_normal_primary_key(self, tmpdir, table_name, attr_names, data_matrix, expected):
        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")
        table_name = TEST_TABLE_NAME

        con.create_table_from_data_matrix(
            table_name, attr_names, data_matrix, primary_key=attr_names[0]
        )

        assert con.schema_extractor.fetch_table_schema(table_name).primary_key == "AA"

    def test_normal_add_primary_key_column(self, tmpdir):
        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")

        table_name = "table1"
        con.create_table_from_data_matrix(
            table_name=table_name,
            attr_names=["AA", "BB"],
            data_matrix=[["a", 11], ["bb", 12]],
            add_primary_key_column=True,
        )
        assert con.select_as_tabledata(table_name) == TableData(
            table_name=table_name, headers=["id", "AA", "BB"], rows=[[1, "a", 11], [2, "bb", 12]]
        )
        assert con.schema_extractor.fetch_table_schema(table_name).primary_key == "id"

        table_name = "table2"
        con.create_table_from_data_matrix(
            table_name=table_name,
            attr_names=["AA", "BB"],
            data_matrix=[["a", 11], ["bb", 12]],
            primary_key="pkey",
            add_primary_key_column=True,
        )
        assert con.select_as_tabledata(table_name) == TableData(
            table_name=table_name, headers=["pkey", "AA", "BB"], rows=[[1, "a", 11], [2, "bb", 12]]
        )
        assert con.schema_extractor.fetch_table_schema(table_name).primary_key == "pkey"

    def test_except_add_primary_key_column(self, tmpdir):
        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")

        with pytest.raises(ValueError):
            con.create_table_from_data_matrix(
                table_name="specify existing attr as a primary key",
                attr_names=["AA", "BB"],
                data_matrix=[["a", 11], ["bb", 12]],
                primary_key="AA",
                add_primary_key_column=True,
            )

    def test_normal_symbol_header(self, tmpdir):
        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")
        table_name = "symbols"
        attr_names = ["a!bc#d$e%f&gh(i)j", "k@l[m]n{o}p;q:r_s.t/u"]
        data_matrix = [{"ABCD>8.5": "aaa", "ABCD<8.5": 0}, {"ABCD>8.5": "bbb", "ABCD<8.5": 9}]
        expected = ["a!bc#d$e%f&gh(i)j", "k@l[m]n{o}p;q:r_s.t/u"]

        con.create_table_from_data_matrix(table_name, attr_names, data_matrix)

        assert con.fetch_attr_names(table_name) == expected

    def test_normal_number_header(self, tmpdir):
        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")
        table_name = "numbers"
        attr_names = [1, 123456789]
        data_matrix = [[1, 2], [1, 2]]
        expected = ["1", "123456789"]

        con.create_table_from_data_matrix(table_name, attr_names, data_matrix)

        assert con.fetch_attr_names(table_name) == expected

    def test_exception_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.create_table_from_data_matrix(TEST_TABLE_NAME, [], [])


class Test_SimpleSQLite_create_table_from_tabledata:
    @pytest.mark.parametrize(
        ["value", "expected"],
        [
            [
                TableData(
                    "json1",
                    ["attr_a", "attr_b", "attr_c"],
                    [
                        {"attr_a": 1, "attr_b": 4, "attr_c": "a"},
                        {"attr_a": 2, "attr_b": 2.1, "attr_c": "bb"},
                        {"attr_a": 3, "attr_b": 120.9, "attr_c": "ccc"},
                    ],
                ),
                [(1, 4.0, "a"), (2, 2.1, "bb"), (3, 120.9, "ccc")],
            ],
            [
                TableData(
                    "multibyte_char",
                    ["姓", "名", "生年月日", "郵便番号", "住所", "電話番号"],
                    [
                        ["山田", "太郎", "2001/1/1", "100-0002", "東京都千代田区皇居外苑", "03-1234-5678"],
                        ["山田", "次郎", "2001/1/2", "251-0036", "神奈川県藤沢市江の島１丁目", "03-9999-9999"],
                    ],
                ),
                [
                    ("山田", "太郎", "2001/1/1", "100-0002", "東京都千代田区皇居外苑", "03-1234-5678"),
                    ("山田", "次郎", "2001/1/2", "251-0036", "神奈川県藤沢市江の島１丁目", "03-9999-9999"),
                ],
            ],
        ],
    )
    def test_normal(self, tmpdir, value, expected):
        p_db = tmpdir.join("tmp.db")

        con = SimpleSQLite(str(p_db), "w")
        con.create_table_from_tabledata(value)

        assert con.fetch_table_names() == [value.table_name]
        assert con.fetch_attr_names(value.table_name) == value.headers

        result = con.select(select="*", table_name=value.table_name)
        result_matrix = result.fetchall()
        assert result_matrix == expected

        actual = con.select_as_tabledata(columns=value.headers, table_name=value.table_name)
        assert actual.equals(value)


class Test_SimpleSQLite_select_as_tabledata:
    @pytest.mark.parametrize(
        ["value", "type_hints", "expected"],
        [
            [
                TableData(
                    "typehints",
                    ["aa", "ab", "ac"],
                    [[1, 4, "10"], [2, 2.1, "11"], [3, 120.9, "12"]],
                ),
                {"aa": typepy.String, "cc": typepy.Integer},
                [["1", 4, 10], ["2", Decimal("2.1"), 11], ["3", Decimal("120.9"), 12]],
            ]
        ],
    )
    def test_normal(self, tmpdir, value, type_hints, expected):
        p_db = tmpdir.join("tmp.db")

        con = SimpleSQLite(str(p_db), "w")
        con.create_table_from_tabledata(value)

        assert con.fetch_table_names() == [value.table_name]
        assert con.fetch_attr_names(value.table_name) == value.headers

        actual = con.select_as_tabledata(
            columns=value.headers, table_name=value.table_name, type_hints=type_hints
        )
        assert actual.value_matrix == expected


class Test_SimpleSQLite_create_table_from_csv:
    @pytest.mark.parametrize(
        [
            "csv_text",
            "csv_filename",
            "table_name",
            "attr_names",
            "expected_table_name",
            "expected_attr_names",
            "expected_data_matrix",
        ],
        [
            [
                "\n".join(['1,4,"a"', '2,2.1,"bb"', '3,120.9,"ccc"']),
                "tmp.csv",
                "tablename",
                ["hoge", "foo", "bar"],
                "tablename",
                ["hoge", "foo", "bar"],
                [(1, 4.0, "a"), (2, 2.1, "bb"), (3, 120.9, "ccc")],
            ],
            [
                "\n".join(['"attr_a","attr_b","attr_c"', '1,4,"a"', '2,2.1,"bb"', '3,120.9,"ccc"']),
                "tmp.csv",
                "",
                [],
                "tmp",
                ["attr_a", "attr_b", "attr_c"],
                [(1, 4.0, "a"), (2, 2.1, "bb"), (3, 120.9, "ccc")],
            ],
        ],
    )
    def test_normal_file(
        self,
        tmpdir,
        csv_text,
        csv_filename,
        table_name,
        attr_names,
        expected_table_name,
        expected_attr_names,
        expected_data_matrix,
    ):
        p_db = tmpdir.join("tmp.db")
        p_csv = tmpdir.join(csv_filename)

        with open(str(p_csv), "w") as f:
            f.write(csv_text)

        con = SimpleSQLite(str(p_db), "w")
        try:
            con.create_table_from_csv(str(p_csv), table_name, attr_names)
        except ImportError:
            pytest.skip("requires pytablereader")

        assert con.fetch_table_names() == [expected_table_name]
        assert expected_attr_names == con.fetch_attr_names(expected_table_name)

        result = con.select(select="*", table_name=expected_table_name)
        result_matrix = result.fetchall()
        assert len(result_matrix) == 3
        assert result_matrix == expected_data_matrix

    @pytest.mark.parametrize(
        [
            "csv_text",
            "table_name",
            "attr_names",
            "expected_table_name",
            "expected_attr_names",
            "expected_data_matrix",
        ],
        [
            [
                "\n".join(['"attr_a","attr_b","attr_c"', '1,4,"a"', '2,2.1,"bb"', '3,120.9,"ccc"']),
                "tmp",
                [],
                "tmp",
                ["attr_a", "attr_b", "attr_c"],
                [(1, 4.0, "a"), (2, 2.1, "bb"), (3, 120.9, "ccc")],
            ]
        ],
    )
    def test_normal_text(
        self,
        tmpdir,
        csv_text,
        table_name,
        attr_names,
        expected_table_name,
        expected_attr_names,
        expected_data_matrix,
    ):
        p_db = tmpdir.join("tmp.db")

        con = SimpleSQLite(str(p_db), "w")
        try:
            con.create_table_from_csv(csv_text, table_name, attr_names)
        except ImportError:
            pytest.skip("requires pytablereader")

        assert con.fetch_table_names() == [expected_table_name]
        assert expected_attr_names == con.fetch_attr_names(expected_table_name)

        result = con.select(select="*", table_name=expected_table_name)
        result_matrix = result.fetchall()
        assert len(result_matrix) == 3
        assert result_matrix == expected_data_matrix


class Test_SimpleSQLite_create_table_from_json:
    @pytest.mark.parametrize(
        [
            "json_text",
            "filename",
            "table_name",
            "expected_table_name",
            "expected_attr_names",
            "expected_data_matrix",
        ],
        [
            [
                """[
                    {"attr_b": 4, "attr_c": "a", "attr_a": 1},
                    {"attr_b": 2.1, "attr_c": "bb", "attr_a": 2},
                    {"attr_b": 120.9, "attr_c": "ccc", "attr_a": 3}
                ]""",
                "tmpjson.json",
                "",
                "tmpjson",
                ["attr_a", "attr_b", "attr_c"],
                [(1, 4.0, "a"), (2, 2.1, "bb"), (3, 120.9, "ccc")],
            ],
            [
                """{
                    "tablename" : [
                        {"attr_b": 4, "attr_c": "a", "attr_a": 1},
                        {"attr_b": 2.1, "attr_c": "bb", "attr_a": 2},
                        {"attr_b": 120.9, "attr_c": "ccc", "attr_a": 3}
                    ]
                }""",
                "tmp.json",
                "%(filename)s_%(key)s",
                "tmp_tablename",
                ["attr_a", "attr_b", "attr_c"],
                [(1, 4.0, "a"), (2, 2.1, "bb"), (3, 120.9, "ccc")],
            ],
            [
                """{
                    "tablename" : [
                        {"attr_b": 4, "attr_c": "a", "attr_a": 1},
                        {"attr_b": 2.1, "attr_c": "bb", "attr_a": 2},
                        {"attr_b": 120.9, "attr_c": "ccc", "attr_a": 3}
                    ]
                }""",
                "tmp.json",
                "",
                "tablename",
                ["attr_a", "attr_b", "attr_c"],
                [(1, 4.0, "a"), (2, 2.1, "bb"), (3, 120.9, "ccc")],
            ],
        ],
    )
    def test_normal_file(
        self,
        tmpdir,
        json_text,
        filename,
        table_name,
        expected_table_name,
        expected_attr_names,
        expected_data_matrix,
    ):
        p_db = tmpdir.join("tmp.db")
        p_json = tmpdir.join(filename)

        with open(str(p_json), "w") as f:
            f.write(json_text)

        con = SimpleSQLite(str(p_db), "w")
        try:
            con.create_table_from_json(str(p_json), table_name)
        except ImportError:
            pytest.skip("requires pytablereader")

        assert con.fetch_table_names() == [expected_table_name]
        assert expected_attr_names == con.fetch_attr_names(expected_table_name)

        result = con.select(select="*", table_name=expected_table_name)
        result_matrix = result.fetchall()
        assert len(result_matrix) == 3
        assert result_matrix == expected_data_matrix

    @pytest.mark.parametrize(
        [
            "json_text",
            "table_name",
            "expected_table_name",
            "expected_attr_names",
            "expected_data_matrix",
        ],
        [
            [
                """[
                    {"attr_b": 4, "attr_c": "a", "attr_a": 1},
                    {"attr_b": 2.1, "attr_c": "bb", "attr_a": 2},
                    {"attr_b": 120.9, "attr_c": "ccc", "attr_a": 3}
                ]""",
                "tmp",
                "tmp",
                ["attr_a", "attr_b", "attr_c"],
                [(1, 4.0, "a"), (2, 2.1, "bb"), (3, 120.9, "ccc")],
            ],
            [
                """{
                    "tablename" : [
                        {"attr_b": 4, "attr_c": "a", "attr_a": 1},
                        {"attr_b": 2.1, "attr_c": "bb", "attr_a": 2},
                        {"attr_b": 120.9, "attr_c": "ccc", "attr_a": 3}
                    ]
                }""",
                "",
                "tablename",
                ["attr_a", "attr_b", "attr_c"],
                [(1, 4.0, "a"), (2, 2.1, "bb"), (3, 120.9, "ccc")],
            ],
        ],
    )
    def test_normal_text(
        self,
        tmpdir,
        json_text,
        table_name,
        expected_table_name,
        expected_attr_names,
        expected_data_matrix,
    ):
        p_db = tmpdir.join("tmp.db")

        con = SimpleSQLite(str(p_db), "w")
        try:
            con.create_table_from_json(json_text, table_name)
        except ImportError:
            pytest.skip("requires pytablereader")

        assert con.fetch_table_names() == [expected_table_name]
        assert expected_attr_names == con.fetch_attr_names(expected_table_name)

        result = con.select(select="*", table_name=expected_table_name)
        result_matrix = result.fetchall()
        assert len(result_matrix) == 3
        assert result_matrix == expected_data_matrix


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


class Test_SimpleSQLite_create_index:

    CHARS = [
        "/",
        ":",
        "*",
        "?",
        '"',
        "<",
        ">",
        "|",
        "\\",
        "!",
        "#",
        "&",
        "'",
        "=",
        "~",
        "^",
        "@",
        "`",
        "[",
        "]",
        "+",
        "-",
        ";",
        "{",
        "}",
        ",",
        ".",
        "(",
        ")",
        "%",
        " ",
        "\t",
        "\n",
        "\r",
        "\f",
        "\v",
    ]

    @pytest.mark.parametrize(["symbol"], [[c] for c in CHARS])
    def test_normal(self, con, symbol):
        attr = "a{}b".format(symbol)
        attr_descriptions = ["{:s} {:s}".format(Attr(attr), "TEXT")]

        table_name = "new_table"
        con.create_table(table_name, attr_descriptions)
        con.create_index(table_name, attr)

    def test_null(self, con_null):
        with pytest.raises(NullDatabaseConnectionError):
            con_null.create_index(TEST_TABLE_NAME, "dummy")


class Test_SimpleSQLite_fetch_num_records:
    def test_null(self, con):
        assert con.fetch_num_records("not_exist") is None
