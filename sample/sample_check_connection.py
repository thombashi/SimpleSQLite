#!/usr/bin/env python
# encoding: utf-8


from simplesqlite import SimpleSQLite, NullDatabaseConnectionError
import six


con = SimpleSQLite("sample.sqlite", "w")

six.print_("---- connected to a database ----")
con.check_connection()

six.print_("---- disconnected from a database ----")
con.close()
try:
    con.check_connection()
except NullDatabaseConnectionError as e:
    six.print_(e)
