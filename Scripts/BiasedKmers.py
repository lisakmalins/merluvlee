# Lisa Malins
# 13 Jan 2020
# BiasedKmers.py
"""
Queries database for k-mers above a certain bias threshold.
Writes sequences and counts to CSV.

Usage: python BiasedKmers.py input.db {1/2} output.csv {cutoff}
1 selects k-mers present in first set and absent in second.
2 selects k-mers present in second set and absent in first.
"""

import sys
import os
import sqlite3

usage = "Usage: python BiasedKmers.py input.db {1/2} output.csv {cutoff}\n\
\n1 selects k-mers present in first set and absent in second. \
\n2 selects k-mers present in second set and absent in first.\n"

# Verify argument number
if len(sys.argv) < 4:
    sys.stderr.write("Too few arguments.\n{}".format(usage))
    exit(1)

# Set low coverage cutoff for biased k-mers
if len(sys.argv) == 5:
    if sys.argv[4].isdigit():
        cutoff = sys.argv[4]
    else:
        sys.stderr.write("Expected a number for low coverage cutoff.\n{}".format(usage))
        exit(1)
# Default to 1 if not specified by user
else:
    cutoff = "1"


# Set query condition
# Present in 1st, absent in 2nd
if sys.argv[2] == "1":
    condition = "count2 <= 1 AND count1 >= {}".format(cutoff)
# Present in 2nd, absent in 1st
elif sys.argv[2] == "2":
    condition = "count1 <= 1 AND count2 >= {}".format(cutoff)
# User error
else:
    sys.stderr.write("Must specify either 1 or 2 to select bias\n{}".format(usage))
    exit(1)


# Die if database doesn't exist so SQL doesn't create empty database
if not os.path.isfile(sys.argv[1]):
    sys.stderr.write("File {} does not exist.\n{}".format(str(sys.argv[1]), usage))
    exit(1)

# Echo coverage cutoff info to user
sys.stderr.write("Low coverage cutoff at {}\n".format(cutoff))

# Connect and create cursor
connection = sqlite3.connect(sys.argv[1])
cursor = connection.cursor()

# Open output CSV
with open(sys.argv[3], 'w') as output:
    sys.stderr.write("Writing biased k-mers from file {} to {}\n".format(sys.argv[1], output.name))

    # Grab contents
    cursor.execute("SELECT seq, count1, count2 FROM compare_kmers WHERE {}".format(condition))

    # Print contents
    row = cursor.fetchone()
    while row:
        output.write("{},{},{}\n".format(row[0], row[1], row[2]))
        row = cursor.fetchone()

sys.stderr.write("Done dumping counts from file {} to {}\n".format(sys.argv[1], output.name))
