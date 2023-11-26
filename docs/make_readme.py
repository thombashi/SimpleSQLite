#!/usr/bin/env python3

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

import sys

from path import Path
from readmemaker import ReadmeMaker


PROJECT_NAME = "SimpleSQLite"
OUTPUT_DIR = ".."


def write_examples(maker):
    maker.set_indent_level(0)
    maker.write_chapter("Examples")

    examples_root = Path("pages").joinpath("examples")

    maker.inc_indent_level()
    maker.write_chapter("Create a table")

    with maker.indent():
        maker.write_chapter("Create a table from a data matrix")
        maker.write_file(examples_root.joinpath("create_table/create_table_from_data_matrix.txt"))

        maker.write_chapter("Create a table from CSV")
        maker.write_file(examples_root.joinpath("create_table/create_table_from_csv.txt"))

        maker.write_chapter("Create a table from pandas.DataFrame")
        maker.write_file(examples_root.joinpath("create_table/create_table_from_df.txt"))

    maker.write_chapter("Insert records into a table")
    maker.write_file(examples_root.joinpath("insert_record_example.txt"))

    maker.write_chapter("Fetch data from a table as pandas DataFrame")
    maker.write_file(examples_root.joinpath("select_as/select_as_dataframe.txt"))

    maker.write_chapter("ORM functionality")
    maker.write_file(examples_root.joinpath("orm/orm_model.txt"))

    maker.write_chapter("For more information")
    maker.write_lines(
        [
            "More examples are available at ",
            f"https://{PROJECT_NAME.lower():s}.rtfd.io/en/latest/pages/examples/index.html",
        ]
    )


def main() -> None:
    maker = ReadmeMaker(
        PROJECT_NAME,
        OUTPUT_DIR,
        is_make_toc=True,
        project_url=f"https://github.com/thombashi/{PROJECT_NAME}",
    )

    maker.write_chapter("Summary")
    maker.write_introduction_file("summary.txt")
    maker.write_introduction_file("badges.txt")
    maker.write_introduction_file("feature.txt")

    write_examples(maker)

    maker.write_introduction_file("installation.rst")

    maker.set_indent_level(0)
    maker.write_chapter("Documentation")
    maker.write_lines([f"https://{PROJECT_NAME.lower():s}.rtfd.io/"])

    maker.write_chapter("Related Project")
    maker.write_lines(
        [
            "- `sqlitebiter <https://github.com/thombashi/sqlitebiter>`__: "
            "CLI tool to convert CSV/Excel/HTML/JSON/LTSV/Markdown/TSV/Google-Sheets "
            "SQLite database by using SimpleSQLite"
        ]
    )

    maker.write_file(maker.doc_page_root_dir_path.joinpath("sponsors.rst"))

    return 0


if __name__ == "__main__":
    sys.exit(main())
