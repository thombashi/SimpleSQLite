#!/usr/bin/env python3

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from simplesqlite import connect_memdb
from simplesqlite.model import Blob, Integer, Model, Real, Text


class Hoge(Model):
    hoge_id = Integer()
    name = Text()


class Foo(Model):
    foo_id = Integer(not_null=True)
    name = Text(not_null=True)
    value = Real(not_null=True)
    blob = Blob()


def main():
    con = connect_memdb()

    Hoge.attach(con, is_hidden=True)
    Hoge.create()
    Hoge.insert(Hoge(hoge_id=10, name="a"))
    Hoge.insert(Hoge(hoge_id=20, name="b"))

    Foo.attach(con)
    Foo.create()
    Foo.insert(Foo(foo_id=11, name="aq", value=0.1))
    Foo.insert(Foo(foo_id=22, name="bb", value=1.11))

    print(Hoge.fetch_schema().dumps())
    for hoge in Hoge.select():
        print(hoge.hoge_id, hoge.name)
    print("\n--------------------\n")
    print(Foo.fetch_schema().dumps())
    for foo in Foo.select():
        print(foo)


if __name__ == "__main__":
    main()
