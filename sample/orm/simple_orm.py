#!/usr/bin/env python
# encoding: utf-8

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from __future__ import print_function, unicode_literals

import sys

from simplesqlite import connect_memdb
from simplesqlite.model import Integer, Model, Real, Text


class Sample(Model):
    foo_id = Integer(not_null=True)
    name = Text(not_null=True, unique=True)
    value = Real()


def main():
    con = connect_memdb()

    Sample.attach(con)
    Sample.create()
    Sample.insert(Sample(foo_id=11, name="abc", value=0.1))
    Sample.insert(Sample(foo_id=22, name="xyz", value=1.11))

    print(Sample.fetch_schema().dumps())
    print("records:")
    for record in Sample.select():
        print("  {}".format(record))

    return 0


if __name__ == "__main__":
    sys.exit(main())