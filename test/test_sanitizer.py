# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import unicode_literals

import pytablewriter as ptw
import pytest
import tabledata
from simplesqlite import SQLiteTableDataSanitizer
from tabledata import TableData


class Test_SQLiteTableDataSanitizer(object):

    @pytest.mark.parametrize(
        ["table_name", "header_list", "record_list", "expected"],
        [
            [
                "normal", ["a", "b_c"], [[1, 2], [3, 4]],
                TableData("normal", ["a", "b_c"], [[1, 2], [3, 4]])
            ], [
                "underscore_char", ["data", "_data", "data_", "_data_"],
                [[1, 2, 3, 4], [11, 12, 13, 14]],
                TableData(
                    "underscore_char", ["data", "_data", "data_", "_data_"],
                    [[1, 2, 3, 4], [11, 12, 13, 14]])
            ], [
                "OFFSET", ["abort", "ASC"], [[1, 2], [3, 4]],
                TableData("OFFSET", ["abort", "ASC"], [[1, 2], [3, 4]])
            ], [
                "missing_all_header", [], [[1, 2], [3, 4]],
                TableData("missing_all_header", ["A", "B"], [[1, 2], [3, 4]])
            ], [
                "missing_part_of_header", ["", "bb", None], [],
                TableData("missing_part_of_header", ["A", "bb", "C"], [])
            ], [
                "avoid_duplicate_default_header_0", ["", "a", None], [],
                TableData("avoid_duplicate_default_header_0", ["B", "a", "C"], [])
            ], [
                "avoid_duplicate_default_header_1",
                ["", "A", "B", "c", ""], [],
                TableData(
                    "avoid_duplicate_default_header_1",
                    ["D", "A", "B", "c", "E"], [])
            ], [
                r"@a!b\c#d$e%f&g'h(i)j_",
                [r"a!bc#d$e%f&g'h(i)j", r"k@l[m]n{o}p;q:r,s.t/u", "a b"],
                [[1, 2, 3], [11, 12, 13]],
                TableData(
                    "a_b_c_d_e_f_g_h_i_j",
                    ["a!bc#d$e%f&g_h(i)j", "k@l[m]n{o}p;q:r_s.t/u", "a b"],
                    [[1, 2, 3], [11, 12, 13]])
            ], [
                # SQLite reserved keywords
                "ALL", ["and", "Index"], [[1, 2], [3, 4]],
                TableData("rename_ALL", ["and", "Index"], [[1, 2], [3, 4]])
            ], [
                "invalid'tn", ["in'valid", "ALL"], [[1, 2], [3, 4]],
                TableData(
                    "invalid_tn",
                    ["in_valid", "ALL"], [[1, 2], [3, 4]])
            ], [
                "Python (programming language) - Wikipedia, the free encyclopedia.html",
                ["a b", "c d"], [[1, 2], [3, 4]],
                TableData(
                    "Python_programming_language_Wikipedia_the_free_encyclopedia_html",
                    ["a b", "c d"], [[1, 2], [3, 4]])
            ], [
                "multibyte csv",
                ["姓", "名", "生年月日", "郵便番号", "住所", "電話番号"],
                [
                    ["山田", "太郎", "2001/1/1", "100-0002",
                        "東京都千代田区皇居外苑", "03-1234-5678"],
                    ["山田", "次郎", "2001/1/2", "251-0036",
                        "神奈川県藤沢市江の島１丁目", "03-9999-9999"],
                ],
                TableData(
                    "multibyte_csv",
                    ["姓", "名", "生年月日", "郵便番号", "住所", "電話番号"],
                    [
                        ["山田", "太郎", "2001/1/1", "100-0002",
                         "東京都千代田区皇居外苑", "03-1234-5678"],
                        ["山田", "次郎", "2001/1/2", "251-0036",
                         "神奈川県藤沢市江の島１丁目", "03-9999-9999"],
                    ])
            ],
        ])
    def test_normal(self, table_name, header_list, record_list, expected):
        new_tabledata = sanitizer.sanitize()
        sanitizer = SQLiteTableDataSanitizer(TableData(table_name, header_list, record_list))

        print("lhs: {}".format(ptw.dump_tabledata(new_tabledata)))
        print("rhs: {}".format(ptw.dump_tabledata(expected)))

        assert new_tabledata == expected

    @pytest.mark.parametrize(
        ["table_name", "header_list", "record_list", "expected"], [
            ["", ["a", "b"], [], tabledata.InvalidTableNameError],
            [None, ["a", "b"], [], tabledata.InvalidTableNameError],
            ["dummy", [], [], tabledata.DataError],
        ])
    def test_exception_invalid_data(self, table_name, header_list, record_list, expected):
        sanitizer = SQLiteTableDataSanitizer(TableData(table_name, header_list, record_list))

        with pytest.raises(expected):
            sanitizer.sanitize()
