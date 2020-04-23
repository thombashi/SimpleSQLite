#!/usr/bin/env python3

from simplesqlite import AttributeNotFoundError, DatabaseError, SimpleSQLite


def main():
    table_name = "sample_table"
    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_from_data_matrix(table_name, ["attr_a", "attr_b"], [[1, "a"], [2, "b"]])

    con.verify_attr_existence(table_name, "attr_a")
    try:
        con.verify_attr_existence(table_name, "not_existing")
    except AttributeNotFoundError as e:
        print(e)
    try:
        con.verify_attr_existence("not_existing", "attr_a")
    except DatabaseError as e:
        print(e)


if __name__ == "__main__":
    main()
