"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

import os

import pytest

from simplesqlite import SimpleSQLite, SQLiteTableDataSanitizer


class Test_SimpleSQLite_create_table_from_tabledata:
    @pytest.mark.parametrize(["filename"], [["python - Wiktionary.html"]])
    def test_smoke(self, tmpdir, filename):
        try:
            import pytablereader as ptr
        except ImportError:
            pytest.skip("requires pytablereader")

        p = tmpdir.join("tmp.db")
        con = SimpleSQLite(str(p), "w")

        test_data_file_path = os.path.join(os.path.dirname(__file__), "data", filename)
        loader = ptr.TableFileLoader(test_data_file_path)

        success_count = 0

        for table_data in loader.load():
            if table_data.is_empty():
                continue

            try:
                from pytablewriter import dumps_tabledata

                print(dumps_tabledata(table_data))
            except ImportError:
                pass

            try:
                con.create_table_from_tabledata(SQLiteTableDataSanitizer(table_data).normalize())
                success_count += 1
            except ValueError as e:
                print(e)

        con.commit()

        assert success_count > 0
