#!/usr/bin/env python
# encoding: utf-8

import os
import sys
import subprocess


VERSION = "0.2.0"
OUTPUT_DIR = ".."
README_WORK_DIR = "."
DOC_PAGE_DIR = os.path.join(README_WORK_DIR, "pages")


def main():
    with open(os.path.join(OUTPUT_DIR, "README.rst"), "w") as f:
        f.write("\n".join([
            line.rstrip() for line in
            open(os.path.join(DOC_PAGE_DIR, "introduction.rst")).readlines()
        ]))
        f.write("\n" * 2)

        f.write("\n".join([
            "Usage",
            "=====",
            "",
            "Create a table",
            "--------------",
            "",
            "Create a table from data matrix",
            "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~",
        ]))
        f.write("\n" * 2)
        f.write("\n".join([
            line.rstrip()
            for line in
            open(os.path.join(
                DOC_PAGE_DIR, "usage",
                "create_table_from_data_matrix.rst")).readlines()
        ]))
        f.write("\n" * 2)

        f.write("\n".join([
            "Insert records into a table",
            "---------------------------",
        ]))
        f.write("\n" * 2)
        f.write("\n".join([
            line.rstrip()
            for line in
            open(os.path.join(
                DOC_PAGE_DIR, "usage",
                "insert_record_example.rst")).readlines()
        ]))
        f.write("\n" * 2)

        f.write("\n".join([
            line.rstrip() for line in
            open(os.path.join(DOC_PAGE_DIR, "installation.rst")).readlines()
        ]))
        f.write("\n" * 2)

        f.write("\n".join([
            "Documentation",
            "=============",
            "",
            "http://simplesqlite.readthedocs.org/en/latest/"
        ]))
        f.write("\n" * 2)
if __name__ == '__main__':
    sys.exit(main())
