# Lisa Malins
# 3 Nov 2019
# KmerDatabase.py
"""
Gets k-mers and counts from jellyfish dump file and inserts in database.
Usage: python KmerDatabase.py output.db dump1.fa dump2.fa
"""
import sys
from os import stat
import sqlite3
from time import ctime

# Verify argument number
usage = "Usage: python KmerDatabase.py output.db dump1.fa dump2.fa\n"
if len(sys.argv) != 4:
    sys.stderr.write(usage)
    exit(1)

# Open input
try:
    dump1 = open(sys.argv[2], 'r')
    dump2 = open(sys.argv[3], 'r')
except FileNotFoundError as e:
    sys.stderr.write("File {} not found.\n{}".format(e.filename, usage))
    exit(1)

# Connect to database
connection = sqlite3.connect(sys.argv[1])
cursor = connection.cursor()

# Create table
cursor.execute('DROP TABLE IF EXISTS kmers')
cursor.execute('CREATE TABLE kmers (seq TEXT PRIMARY KEY, count1 INTEGER, count2 INTEGER)')

# Populate table from files
for source in (dump1, dump2):
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

        # First pass: Insert each k-mer and count (set other count to 1)
        if source.name == dump1.name:
            cursor.execute('INSERT INTO kmers (seq, count1, count2) VALUES (?, ?, ?)', (seq, count, 1))

        # Second pass: Update entries with counts in second file or make new entry if needed
        else:
            # Grab count1 from row if k-mer already exists in database
            cursor.execute('SELECT count1 FROM kmers where seq=?', (seq ,))

            # Unpack count (this will throw TypeError if no row selected)
            try:
                (othercount, ) = cursor.fetchone()
                # print("Sequence {} already in table, count is {}".format(seq, othercount)) #debug

            # K-mer was not in other file so fill in count of 1
            except TypeError:
                othercount = 1

            # Inser tor replace as appropriate
            cursor.execute('INSERT OR REPLACE INTO kmers (seq, count1, count2) VALUES (?, ?, ?)', \
            (seq, othercount, count))

    # Commit changes
    sys.stderr.write("Committing changes to database, this may take some time\t\t{}\n".format(ctime()))
    connection.commit()

sys.stderr.write("Done populating database {}\t\t{}\n".format(sys.argv[1], ctime()))
