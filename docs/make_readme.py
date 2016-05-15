#!/usr/bin/env python
# encoding: utf-8

import os
import re
import sys


VERSION = "0.4.1"
OUTPUT_DIR = ".."
README_WORK_DIR = "."
DOC_PAGE_DIR = os.path.join(README_WORK_DIR, "pages")


def get_usage_file_path(filename):
    return os.path.join(DOC_PAGE_DIR, "examples", filename)


def write_line_list(f, line_list):
    f.write("\n".join([
        line for line in line_list
        if re.search(":caption:", line) is None
    ]))
    f.write("\n" * 2)


def write_usage_file(f, filename):
    write_line_list(f, [
        line.rstrip()
        for line in
        open(get_usage_file_path(filename)).readlines()
    ])


def write_examples(f):
    write_line_list(f, [
        "Examples",
        "========",
        "",
        "Create a table",
        "--------------",
        "",
        "Create a table from data matrix",
        "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~",
    ])

    write_usage_file(f, "create_table_from_data_matrix.txt")

    write_line_list(f, [
        "Insert records into a table",
        "---------------------------",
    ])

    write_usage_file(f, "insert_record_example.txt")

    write_line_list(f, [
        "For more information",
        "--------------------",
        "More examples are available at ",
        "http://simplesqlite.readthedocs.org/en/latest/pages/examples/index.html",
        "",
    ])


def main():
    with open(os.path.join(OUTPUT_DIR, "README.rst"), "w") as f:
        write_line_list(f, [
            "SimpleSQLite",
            "=============",
            "",
        ] + [
            line.rstrip() for line in
            open(os.path.join(
                DOC_PAGE_DIR, "introduction", "badges.txt")).readlines()
        ])

        write_line_list(f, [
            "Summary",
            "-------",
            "",
        ] + [
            line.rstrip() for line in
            open(os.path.join(
                DOC_PAGE_DIR, "introduction", "summary.txt")).readlines()
        ])

        write_line_list(f, [
            line.rstrip() for line in
            open(os.path.join(
                DOC_PAGE_DIR, "introduction", "feature.txt")).readlines()
        ])

        write_examples(f)

        write_line_list(f, [
            line.rstrip() for line in
            open(os.path.join(DOC_PAGE_DIR, "installation.rst")).readlines()
        ])

        write_line_list(f, [
            "Documentation",
            "=============",
            "",
            "http://simplesqlite.readthedocs.org/en/latest/"
        ])

        write_line_list(f, [
            "Related project",
            "==========================",
            "",
            "- sqlitebiter: CLI tool to create a SQLite database from CSV/JSON/Excel/Google-Sheets by using SimpleSQLite",
            "    - https://github.com/thombashi/sqlitebiter"
        ])

    sys.stdout.write("complete\n")
    sys.stdout.flush()
    sys.stdin.readline()

if __name__ == '__main__':
    sys.exit(main())
