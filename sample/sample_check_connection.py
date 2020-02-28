#!/usr/bin/env python3

import simplesqlite


con = simplesqlite.SimpleSQLite("sample.sqlite", "w")

print("---- connected to a database ----")
con.check_connection()

print("---- disconnected from a database ----")
con.close()
try:
    con.check_connection()
except simplesqlite.NullDatabaseConnectionError as e:
    print(e)
