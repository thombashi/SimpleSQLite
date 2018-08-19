#!/usr/bin/env python
# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import unicode_literals

import sys

import readmemaker
from path import Path


PROJECT_NAME = "SimpleSQLite"
OUTPUT_DIR = ".."


def write_examples(maker):
    maker.set_indent_level(0)
    maker.write_chapter("Examples")

    examples_root = Path("pages").joinpath("examples")

    maker.inc_indent_level()
    maker.write_chapter("Create a table")
    maker.inc_indent_level()
    maker.write_chapter("Create a table from data matrix")
    maker.write_file(examples_root.joinpath("create_table/create_table_from_data_matrix.txt"))

    maker.write_chapter("Create a table from CSV")
    maker.write_file(examples_root.joinpath("create_table/create_table_from_csv.txt"))

    maker.write_chapter("Create a table from pandas.DataFrame")
    maker.write_file(examples_root.joinpath("create_table/create_table_from_df.txt"))

    maker.dec_indent_level()

    maker.write_chapter("Insert records into a table")
    maker.write_file(examples_root.joinpath("insert_record_example.txt"))

    maker.write_chapter("Fetch data from a table as pandas DataFrame")
    maker.write_file(examples_root.joinpath("select_as/select_as_dataframe.txt"))

    maker.write_chapter("ORM functionality")
    maker.write_file(examples_root.joinpath("orm/orm_model.txt"))

    maker.write_chapter("For more information")
    maker.write_line_list(
        [
            "More examples are available at ",
            "https://{:s}.rtfd.io/en/latest/pages/examples/index.html".format(PROJECT_NAME.lower()),
        ]
    )


def main():
    maker = readmemaker.ReadmeMaker(PROJECT_NAME, OUTPUT_DIR, is_make_toc=True)

    maker.write_chapter("Summary")
    maker.write_introduction_file("summary.txt")
    maker.write_introduction_file("badges.txt")
    maker.write_introduction_file("feature.txt")

    write_examples(maker)

    maker.write_file(maker.doc_page_root_dir_path.joinpath("installation.rst"))

    maker.set_indent_level(0)
    maker.write_chapter("Documentation")
    maker.write_line_list(["https://{:s}.rtfd.io/".format(PROJECT_NAME.lower())])

    maker.write_chapter("Related project")
    maker.write_line_list(
        [
            "- `sqlitebiter <https://github.com/thombashi/sqlitebiter>`__: "
            "CLI tool to convert CSV/Excel/HTML/JSON/LTSV/Markdown/TSV/Google-Sheets "
            "SQLite database by using SimpleSQLite"
        ]
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
