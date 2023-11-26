#!/usr/bin/env python3

import simplesqlite


def main() -> None:
    table_name = "sample_table"
    con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
    con.create_table_from_data_matrix(table_name, ["attr_a", "attr_b"], [[1, "a"], [2, "b"]])

    print(con.has_attrs(table_name, ["attr_a"]))
    print(con.has_attrs(table_name, ["attr_a", "attr_b"]))
    print(con.has_attrs(table_name, ["attr_a", "attr_b", "not_existing"]))
    try:
        print(con.has_attrs("not_existing_table", ["attr_a"]))
    except simplesqlite.DatabaseError as e:
        print(e)


if __name__ == "__main__":
    main()
