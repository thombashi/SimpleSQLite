#!/usr/bin/env python
# encoding: utf-8

import os
import sys


VERSION = "0.4.0"
OUTPUT_DIR = ".."
README_WORK_DIR = "."
DOC_PAGE_DIR = os.path.join(README_WORK_DIR, "pages")


def get_usage_file_path(filename):
    return os.path.join(DOC_PAGE_DIR, "examples", filename)


def write_line_list(f, line_list):
    f.write("\n".join(line_list))
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

    write_usage_file(f, "insert_record_example.rst")

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
            line.rstrip() for line in
            open(os.path.join(DOC_PAGE_DIR, "introduction.rst")).readlines()
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

    sys.stdout.write("complete\n")
    sys.stdout.flush()
    sys.stdin.readline()

if __name__ == '__main__':
    sys.exit(main())
