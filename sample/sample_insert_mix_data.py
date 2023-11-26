#!/usr/bin/env python3

from collections import namedtuple

from simplesqlite import SimpleSQLite


def main() -> None:
    table_name = "sample_table"
    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_from_data_matrix(
        table_name, ["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"], [[1, 1.1, "aaa", 1, 1]]
    )

    # insert namedtuple
    SampleTuple = namedtuple("SampleTuple", "attr_a attr_b attr_c attr_d attr_e")

    con.insert(table_name, record=[7, 7.7, "fff", 7.77, "bar"])
    con.insert_many(
        table_name,
        records=[(8, 8.8, "ggg", 8.88, "foobar"), SampleTuple(9, 9.9, "ggg", 9.99, "hogehoge")],
    )

    # run select clause
    result = con.select(select="*", table_name=table_name)
    assert result
    for record in result.fetchall():
        print(record)


if __name__ == "__main__":
    main()
