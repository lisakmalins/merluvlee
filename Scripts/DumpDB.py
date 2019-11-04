# Lisa Malins
# 3 Nov 2019
# DumpDB.py
"""
Shortcut to dump the contents of an SQL database to standard out.
Probably not good for huge databases.

Usage: python DumpDB.py input.db
"""

import sys
import sqlite3

# Verify argument number
usage = "Usage: python DumpDB.py input.db\n"
if len(sys.argv) != 2:
    sys.stderr.write(usage)
    exit(1)

# Connect to database
connection = sqlite3.connect(sys.argv[1])
cursor = connection.cursor()

# Get list of tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
(tables, ) = cursor.fetchall()

# Dump contents of all tables
for table in tables:
    print("Table name: {}".format(table))
    cursor.execute("SELECT * FROM {}".format(table))
    row = cursor.fetchone()
    while row:
        print(row)
        row = cursor.fetchone()
