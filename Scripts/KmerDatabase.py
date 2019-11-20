# Lisa Malins
# 20 Nov 2019
# KmerDatabase.py
"""
Gets k-mers and counts from jellyfish dump file and inserts in database.
Usage: python KmerDatabase.py dump.fa output.db
"""
import sys
from os import stat
import sqlite3
from time import ctime

# Verify argument number
usage = "Usage: python KmerDatabase.py dump.fa output.db\n"
if len(sys.argv) != 3:
    sys.stderr.write(usage)
    exit(1)

# Open input
try:
    source = open(sys.argv[1], 'r')
except FileNotFoundError as e:
    sys.stderr.write("File {} not found.\n{}".format(e.filename, usage))
    exit(1)

# Connect to database
connection = sqlite3.connect(sys.argv[2])
cursor = connection.cursor()

# Create table
cursor.execute('DROP TABLE IF EXISTS kmers')
cursor.execute('CREATE TABLE kmers (seq TEXT PRIMARY KEY UNIQUE, count INTEGER)')

# Populate table from files
sys.stderr.write("Reading k-mers and counts from {}\t\t{}\n".format(source.name, ctime()))
filelength = float(stat(source.name).st_size)
percent = 10

while True:
    # Read progress counter
    if (source.tell() / filelength * 100) >= percent:
        sys.stderr.write("Read progress: {}%\t\t{}\n".format(percent, ctime()))
        percent += 10

    # Get sequence and count
    count = source.readline().strip("\n")
    if not count: break
    assert count[0] == ">" and count[1:].isdigit(), "Unexpected input from file {}, line was:\n{}".format(source.name, count)
    count = int(count[1:])
    seq = source.readline().strip("\n")
    # print(count, seq) #debug

    cursor.execute('INSERT INTO kmers (seq, count) VALUES (?, ?)', (seq, count))


# Commit changes
sys.stderr.write("Committing changes to database, this may take some time\t\t{}\n".format(ctime()))
connection.commit()

sys.stderr.write("Done populating database {}\t\t{}\n".format(sys.argv[2], ctime()))
