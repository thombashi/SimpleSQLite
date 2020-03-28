#!/usr/bin/env python3

from simplesqlite import SimpleSQLite


def main():
    file_path = "sample_data_single.json"

    # create sample data file ---
    with open(file_path, "w") as f:
        f.write(
            """[
            {"attr_b": 4, "attr_c": "a", "attr_a": 1},
            {"attr_b": 2.1, "attr_c": "bb", "attr_a": 2},
            {"attr_b": 120.9, "attr_c": "ccc", "attr_a": 3}
        ]"""
        )

    # create table ---
    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_from_json(file_path)

    # output ---
    for table_name in con.fetch_table_names():
        print("table: " + table_name)
        print(con.fetch_attr_names(table_name))
        result = con.select(select="*", table_name=table_name)
        for record in result.fetchall():
            print(record)
        print()


if __name__ == '__main__':
    main()
