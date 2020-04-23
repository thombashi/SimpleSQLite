#!/usr/bin/env python3

from simplesqlite import SimpleSQLite


def main():
    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_from_data_matrix("hoge", ["attr_a", "attr_b"], [[1, "a"], [2, "b"]])

    print(con.has_table("hoge"))
    print(con.has_table("not_existing"))


if __name__ == "__main__":
    main()
