#!/usr/bin/env python3

import simplesqlite


def main():
    con = simplesqlite.SimpleSQLite("sample.sqlite", "w")

    print("---- connected to a database ----")
    con.check_connection()

    print("---- disconnected from a database ----")
    con.close()
    try:
        con.check_connection()
    except simplesqlite.NullDatabaseConnectionError as e:
        print(e)


if __name__ == "__main__":
    main()
