#!/usr/bin/env python3

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from typing import Type

from simplesqlite import connect_memdb
from simplesqlite.model import Blob, Integer, Model, Real, Text
from simplesqlite.query import Set, Where


def print_all_records(model: Type[Model]) -> None:
    for record in model.select():
        print(record)


class Hoge(Model):
    hoge_id = Integer()
    name = Text()


class Foo(Model):
    foo_id = Integer(not_null=True)
    name = Text(not_null=True)
    value = Real(not_null=True)
    blob = Blob()
    nullable = Text()
    created_at = Text(default="CURRENT_TIMESTAMP")


def main() -> None:
    con = connect_memdb()

    Hoge.attach(con, is_hidden=True)
    Hoge.create()
    Hoge.insert(Hoge(hoge_id=10, name="a"))
    Hoge.insert(Hoge(hoge_id=20, name="b"))

    Foo.attach(con)
    Foo.create()
    Foo.insert(Foo(foo_id=11, name="aq", value=0.1))
    Foo.insert(Foo(foo_id=22, name="bb", value=1.1))
    Foo.insert(Foo(foo_id=33, name="cc", value=2.2, nullable=None))
    Foo.insert(Foo(foo_id=44, name="dd", value=3.3, nullable="hoge"))

    print(Hoge.fetch_schema().dumps())
    table_name = Hoge.get_table_name()
    print(f"\nSELECT all the records: table={table_name}")
    for hoge in Hoge.select():
        print(hoge.hoge_id, hoge.name)

    print(f"\nSELECT with WHERE: table={table_name}")
    for hoge in Hoge.select(where=Where(Hoge.hoge_id, 10)):
        print(hoge.hoge_id, hoge.name)

    print("\n--------------------\n")
    print(Foo.fetch_schema().dumps())
    table_name = Foo.get_table_name()

    print(f"\nSELECT all the records: table={table_name}")
    print_all_records(Foo)

    print(f"\nDELETE: table={table_name}")
    Foo.delete(where=Where(Foo.foo_id, 22))
    print_all_records(Foo)

    print(f"\nUPDATE: table={table_name}")
    Foo.update(set_query=[Set(Foo.value, 1000)], where=Where(Foo.foo_id, 33))
    print_all_records(Foo)


if __name__ == "__main__":
    main()
