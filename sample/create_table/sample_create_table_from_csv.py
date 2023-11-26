#!/usr/bin/env python3

from simplesqlite import SimpleSQLite


def main() -> None:
    with open("sample_data.csv", "w") as f:
        f.write("\n".join(['"attr_a","attr_b","attr_c"', '1,4,"a"', '2,2.1,"bb"', '3,120.9,"ccc"']))

    # create table ---
    con = SimpleSQLite("sample.sqlite", "w")
    con.create_table_from_csv("sample_data.csv")

    # output ---
    table_name = "sample_data"
    print(con.fetch_attr_names(table_name))
    result = con.select(select="*", table_name=table_name)
    assert result
    for record in result.fetchall():
        print(record)


if __name__ == "__main__":
    main()
