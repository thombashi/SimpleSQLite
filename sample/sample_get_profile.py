#!/usr/bin/env python3

from simplesqlite import SimpleSQLite


def main():
    con = SimpleSQLite("sample.sqlite", "w", profile=True)
    data_matrix = [[1, 1.1, "aaa", 1, 1], [2, 2.2, "bbb", 2.2, 2.2], [3, 3.3, "ccc", 3, "ccc"]]
    con.create_table_from_data_matrix(
        "sample_table", ["a", "b", "c", "d", "e"], data_matrix, index_attrs=["a"]
    )

    for profile in con.get_profile():
        print(profile)


if __name__ == '__main__':
    main()
