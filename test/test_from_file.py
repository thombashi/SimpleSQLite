# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <gogogo.vm@gmail.com>
"""

from __future__ import print_function
import os

import pytest
import pytablereader as ptr
import pytablewriter as ptw

from simplesqlite import *


class Test_SimpleSQLite_create_table_from_tabledata:

    @pytest.mark.parametrize(["filename"], [
        ["python - Wiktionary.html"],
    ])
    def test_smoke(self, tmpdir, filename):
        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")

        test_data_file_path = os.path.join(
            os.path.dirname(__file__), "data", filename)
        loader = ptr.TableFileLoader(test_data_file_path)

        success_count = 0

        for tabledata in loader.load():
            if tabledata.is_empty():
                continue

            print(ptw.dump_tabledata(tabledata))

            try:
                con.create_table_from_tabledata(
                    ptr.SQLiteTableDataSanitizer(tabledata).sanitize())
                success_count += 1
            except ValueError as e:
                print(e)

        con.commit()

        assert success_count > 0
