#!/usr/bin/env python3

import pandas

from simplesqlite import SimpleSQLite


def main():
    con = SimpleSQLite("pandas_df.sqlite")

    con.create_table_from_dataframe(
        pandas.DataFrame(
            [[0, 0.1, "a"], [1, 1.1, "bb"], [2, 2.2, "ccc"]], columns=["id", "value", "name"]
        ),
        table_name="pandas_df",
    )


if __name__ == '__main__':
    main()
