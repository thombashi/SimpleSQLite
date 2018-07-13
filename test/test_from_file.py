# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import print_function, unicode_literals

import os

import pytablereader as ptr
import pytablewriter as ptw
import pytest
from simplesqlite import SimpleSQLite, SQLiteTableDataSanitizer


class Test_SimpleSQLite_create_table_from_tabledata(object):
    @pytest.mark.parametrize(["filename"], [["python - Wiktionary.html"]])
    def test_smoke(self, tmpdir, filename):
        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")

        test_data_file_path = os.path.join(os.path.dirname(__file__), "data", filename)
        loader = ptr.TableFileLoader(test_data_file_path)

        success_count = 0

        for table_data in loader.load():
            if table_data.is_empty():
                continue

            print(ptw.dump_tabledata(table_data))

            try:
                con.create_table_from_tabledata(SQLiteTableDataSanitizer(table_data).normalize())
                success_count += 1
            except ValueError as e:
                print(e)

        con.commit()

        assert success_count > 0
