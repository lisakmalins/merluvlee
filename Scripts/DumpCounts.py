# Lisa Malins
# 20 Nov 2019
# DumpCounts.py
"""
Shortcut to dump the counts from combined k-mer database to CSV.

Usage: python DumpDB.py input.db output.csv
"""

import sys
import os
import sqlite3

# Verify argument number
usage = "Usage: python DumpDB.py input.db output.csv\n"
if len(sys.argv) < 3:
    sys.stderr.write(usage)
    exit(1)

# Die if database doesn't exist so SQL doesn't create empty database
if not os.path.isfile(sys.argv[1]):
    sys.stderr.write("File {} does not exist.\n{}".format(str(sys.argv[1]), usage))
    exit(1)

# Connect and create cursor
connection = sqlite3.connect(sys.argv[1])
cursor = connection.cursor()

# Open output CSV
with open(sys.argv[2], 'w') as output:
    sys.stderr.write("Dumping counts from file {} to {}\n".format(sys.argv[1], output.name))

    # Grab contents
    cursor.execute("SELECT count1, count2 FROM compare_kmers")

    # Print contents
    row = cursor.fetchone()
    while row:
        output.write("{},{}\n".format(row[0], row[1]))
        row = cursor.fetchone()

sys.stderr.write("Done dumping counts from file {} to {}\n".format(sys.argv[1], output.name))
