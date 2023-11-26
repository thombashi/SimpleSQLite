#!/usr/bin/env python3

from simplesqlite import SimpleSQLite
from simplesqlite.query import Where


def main() -> None:
    table_name = "sample_table"
    con = SimpleSQLite("sample.sqlite", "w")

    data_matrix = [[1, "aaa"], [2, "bbb"]]
    con.create_table_from_data_matrix(table_name, ["key", "value"], data_matrix)

    print("---- before update ----")
    result = con.select(select="*", table_name=table_name)
    assert result
    for record in result.fetchall():
        print(record)
    print()

    con.update(table_name, set_query="value = 'ccc'", where=Where(key="key", value=1))

    print("---- after update ----")
    result = con.select(select="*", table_name=table_name)
    assert result
    for record in result.fetchall():
        print(record)


if __name__ == "__main__":
    main()
