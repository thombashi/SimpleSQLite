#!/usr/bin/env python3

from simplesqlite import SimpleSQLite


def main():
    con = SimpleSQLite("sample.sqlite", "w", profile=True)

    con.create_table_from_data_matrix(
        "sample_table",
        ["a", "b", "c", "d", "e"],
        [[1, 1.1, "aaa", 1, 1], [2, 2.2, "bbb", 2.2, 2.2], [3, 3.3, "ccc", 3, "ccc"]],
    )

    print(con.select_as_dataframe(table_name="sample_table"))


if __name__ == '__main__':
    main()
