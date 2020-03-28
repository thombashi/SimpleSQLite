#!/usr/bin/env python3

from simplesqlite import SimpleSQLite


def main():
    table_name = "sample_table"
    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_from_data_matrix(
        table_name, ["attr_a", "attr_b", "attr_c", "attr_d", "attr_e"], [[1, 1.1, "aaa", 1, 1]]
    )

    con.insert(
        table_name,
        record={"attr_a": 4, "attr_b": 4.4, "attr_c": "ddd", "attr_d": 4.44, "attr_e": "hoge"},
    )
    con.insert_many(
        table_name,
        records=[
            {"attr_a": 5, "attr_b": 5.5, "attr_c": "eee", "attr_d": 5.55, "attr_e": "foo"},
            {"attr_a": 6, "attr_c": "fff"},
        ],
    )

    result = con.select(select="*", table_name=table_name)
    for record in result.fetchall():
        print(record)


if __name__ == '__main__':
    main()
