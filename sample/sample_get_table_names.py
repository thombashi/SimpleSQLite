#!/usr/bin/env python3

from simplesqlite import SimpleSQLite


def main() -> None:
    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_from_data_matrix("hoge", ["attr_a", "attr_b"], [[1, "a"], [2, "b"]])
    print(con.fetch_table_names())


if __name__ == "__main__":
    main()
