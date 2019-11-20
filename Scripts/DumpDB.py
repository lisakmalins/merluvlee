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
if len(sys.argv) < 2:
    sys.stderr.write(usage)
    exit(1)


# Connect to database
for file in sys.argv[1:]:
    print("Dumping database from file {}".format(file))
    connection = sqlite3.connect(file)
    cursor = connection.cursor()

    # Get list of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Dump contents of all tables
    for (table, ) in tables:
        # Print table name
        print("\nTable name: {}".format(table))

        # Grab contents
        cursor.execute("SELECT * FROM {}".format(table))

        # Print column names
        col_names = [tuple[0] for tuple in cursor.description]
        print("Column names: {}".format(str(col_names)))

        # Print contents
        row = cursor.fetchone()
        while row:
            print(row)
            row = cursor.fetchone()

    print("\n")
