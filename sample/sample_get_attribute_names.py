#!/usr/bin/env python3

import simplesqlite


def main():
    table_name = "sample_table"
    con = simplesqlite.SimpleSQLite("sample.sqlite", "w")
    con.create_table_from_data_matrix(table_name, ["attr_a", "attr_b"], [[1, "a"], [2, "b"]])

    print(con.fetch_attr_names(table_name))

    try:
        print(con.fetch_attr_names("not_existing"))
    except simplesqlite.DatabaseError as e:
        print(e)


if __name__ == "__main__":
    main()
