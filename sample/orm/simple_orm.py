#!/usr/bin/env python3

"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from simplesqlite import connect_memdb
from simplesqlite.model import Integer, Model, Real, Text


class Sample(Model):
    foo_id = Integer(primary_key=True)
    name = Text(not_null=True, unique=True)
    value = Real(default=0)


def main() -> None:
    con = connect_memdb()

    Sample.attach(con)
    Sample.create()
    Sample.insert(Sample(name="abc", value=0.1))
    Sample.insert(Sample(name="xyz", value=1.11))
    Sample.insert(Sample(name="bar"))

    print(Sample.fetch_schema().dumps())
    print("records:")
    for record in Sample.select():
        print(f"    {record}")


if __name__ == "__main__":
    main()
