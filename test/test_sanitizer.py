# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import unicode_literals

import pytablewriter as ptw
import pytest
from simplesqlite import NameValidationError, SQLiteTableDataSanitizer, connect_memdb
from tabledata import TableData

from ._common import print_test_result


class Test_SQLiteTableDataSanitizer(object):
    @pytest.mark.parametrize(
        ["table_name", "header_list", "record_list", "expected"],
        [
            [
                "normal",
                ["a", "b_c"],
                [[1, 2], [3, 4]],
                TableData("normal", ["a", "b_c"], [[1, 2], [3, 4]]),
            ],
            [
                "underscore_char",
                ["data", "_data", "data_", "_data_"],
                [[1, 2, 3, 4], [11, 12, 13, 14]],
                TableData(
                    "underscore_char",
                    ["data", "_data", "data_", "_data_"],
                    [[1, 2, 3, 4], [11, 12, 13, 14]],
                ),
            ],
            [
                "OFFSET",
                ["abort", "ASC"],
                [[1, 2], [3, 4]],
                TableData("OFFSET", ["abort", "ASC"], [[1, 2], [3, 4]]),
            ],
            [
                "missing_all_header",
                [],
                [[1, 2], [3, 4]],
                TableData("missing_all_header", ["A", "B"], [[1, 2], [3, 4]]),
            ],
            [
                "missing_part_of_header",
                ["", "bb", None],
                [[1, 2, 3]],
                TableData("missing_part_of_header", ["A", "bb", "C"], [[1, 2, 3]]),
            ],
            [
                "avoid_duplicate_default_header_0",
                ["", "a", None],
                [[1, 2, 3]],
                TableData("avoid_duplicate_default_header_0", ["B", "a", "C"], [[1, 2, 3]]),
            ],
            [
                "avoid_duplicate_default_header_1",
                ["", "A", "B", "c", ""],
                [[1, 2, 3, 4, 5]],
                TableData(
                    "avoid_duplicate_default_header_1", ["D", "A", "B", "c", "E"], [[1, 2, 3, 4, 5]]
                ),
            ],
            [
                r"@a!b\c#d$e%f&g'h(i)j_",
                [r"a!bc#d$e%f&g'h(i)j", r"k@l[m]n{o}p;q:r,s.t/u", "a b"],
                [[1, 2, 3], [11, 12, 13]],
                TableData(
                    "a_b_c_d_e_f_g_h_i_j",
                    ["a!bc#d$e%f&g_h(i)j", "k@l[m]n{o}p;q:r_s.t/u", "a b"],
                    [[1, 2, 3], [11, 12, 13]],
                ),
            ],
            [
                # SQLite reserved keywords
                "ALL",
                ["and", "Index"],
                [[1, 2], [3, 4]],
                TableData("rename_ALL", ["and", "Index"], [[1, 2], [3, 4]]),
            ],
            [
                "invalid'tn",
                ["in'valid", "ALL"],
                [[1, 2], [3, 4]],
                TableData("invalid_tn", ["in_valid", "ALL"], [[1, 2], [3, 4]]),
            ],
            [
                "Python (programming language) - Wikipedia, the free encyclopedia.html",
                ["a b", "c d"],
                [[1, 2], [3, 4]],
                TableData(
                    "Python_programming_language_Wikipedia_the_free_encyclopedia_html",
                    ["a b", "c d"],
                    [[1, 2], [3, 4]],
                ),
            ],
            [
                "multibyte csv",
                ["姓", "名", "生年月日", "郵便番号", "住所", "電話番号"],
                [
                    ["山田", "太郎", "2001/1/1", "100-0002", "東京都千代田区皇居外苑", "03-1234-5678"],
                    ["山田", "次郎", "2001/1/2", "251-0036", "神奈川県藤沢市江の島１丁目", "03-9999-9999"],
                ],
                TableData(
                    "multibyte_csv",
                    ["姓", "名", "生年月日", "郵便番号", "住所", "電話番号"],
                    [
                        ["山田", "太郎", "2001/1/1", "100-0002", "東京都千代田区皇居外苑", "03-1234-5678"],
                        ["山田", "次郎", "2001/1/2", "251-0036", "神奈川県藤沢市江の島１丁目", "03-9999-9999"],
                    ],
                ),
            ],
        ],
    )
    def test_normal(self, table_name, header_list, record_list, expected):
        new_tabledata = SQLiteTableDataSanitizer(
            TableData(table_name, header_list, record_list)
        ).normalize()

        print_test_result(
            expected=ptw.dump_tabledata(expected), actual=ptw.dump_tabledata(new_tabledata)
        )

        con = connect_memdb()
        con.create_table_from_tabledata(new_tabledata)
        assert con.select_as_tabledata(new_tabledata.table_name) == expected

        assert new_tabledata.equals(expected)

    @pytest.mark.parametrize(
        ["table_name", "header_list", "record_list", "expected"],
        [
            ["", ["a", "b"], [], NameValidationError],
            [None, ["a", "b"], [], NameValidationError],
            ["dummy", [], [], ValueError],
        ],
    )
    def test_exception_invalid_data(self, table_name, header_list, record_list, expected):
        with pytest.raises(expected):
            SQLiteTableDataSanitizer(TableData(table_name, header_list, record_list)).normalize()


class Test_SQLiteTableDataSanitizer_dup_col_handler(object):
    @pytest.mark.parametrize(
        ["table_name", "header_list", "dup_col_handler", "expected"],
        [
            [
                "all attrs are duplicated",
                ["A", "A", "A", "A", "A"],
                "rename",
                TableData("all_attrs_are_duplicated", ["A", "A_1", "A_2", "A_3", "A_4"], []),
            ],
            [
                "recursively duplicated attrs",
                ["A", "A", "A_1", "A_1", "A_2", "A_1_1", "A_1_1"],
                "recursively_duplicated_attrs",
                TableData(
                    "recursively_duplicated_attrs",
                    ["A", "A_3", "A_1", "A_1_2", "A_2", "A_1_1", "A_1_1_1"],
                    [],
                ),
            ],
        ],
    )
    def test_normal_(self, table_name, header_list, dup_col_handler, expected):
        new_tabledata = SQLiteTableDataSanitizer(
            TableData(table_name, header_list, []), dup_col_handler=dup_col_handler
        ).normalize()

        print_test_result(
            expected=ptw.dump_tabledata(expected), actual=ptw.dump_tabledata(new_tabledata)
        )

        assert new_tabledata.equals(expected)

    @pytest.mark.parametrize(
        ["table_name", "header_list", "expected"],
        [
            ["duplicate columns", ["a", "a"], ValueError],
            ["duplicate columns", ["AA", "b", "AA"], ValueError],
        ],
    )
    def test_exception(self, table_name, header_list, expected):
        with pytest.raises(expected):
            SQLiteTableDataSanitizer(
                TableData(table_name, header_list, []), dup_col_handler="error"
            ).normalize()
