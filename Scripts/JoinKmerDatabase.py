# Lisa Malins
# 20 Nov 2019
# KmerDatabase.py
"""
Gets k-mers and counts from jellyfish dump file and inserts in database.
Usage: python KmerDatabase.py input1.db input2.db output.db
"""
import sys
from os import stat
from os import path
import sqlite3
from time import ctime

# Verify argument number
usage = "Usage: python JoinKmerDatabase.py input1.db input2.db output.db\n"
if len(sys.argv) != 4:
    sys.stderr.write(usage)
    sys.exit(1)

files = sys.argv[1:]

# Verify first two databases already exist
# Otherwise SQLite will create empty database files which could be confusing
for file in files[:-1]:
    if not path.isfile(file):
        sys.stderr.write("File {} not found\n".format(file))
        sys.stderr.write("First two arguments must be pre-existing databases\n{}".format(usage))
        sys.exit(1)

# Connect to new database
connection = sqlite3.connect(files[2])
cursor = connection.cursor()

# Attach existing databases as sample1 and sample2
try:
    for i in range (0, 2):
        file = files[i]
        sample_num = "sample{}".format(str(i + 1))
        sys.stderr.write("Attaching database {}\t\t{}\n".format(file, ctime()))
        cursor.execute("ATTACH DATABASE ? AS {}".format(sample_num), (file, ) )

    sys.stderr.write("Done attaching databases\t\t{}\n".format(ctime()))

except sqlite3.DatabaseError as e:
    sys.stderr.write("SQL error from file {}: {}\n{}".format(file, str(*e.args), usage))
    sys.exit(1)

# Rename "count" to unique column name if necessary
# Alter table operation triggers implicit commit so this only needs to be done once
for s in ["sample1", "sample2"]:
    cursor.execute("SELECT * FROM {}.kmers".format(s))
    col_names = [tuple[0] for tuple in cursor.description]
    if "count" in col_names:
        new_col_name = "count{}".format(s[-1])
        # print(new_col_name) #debug
        cursor.execute("ALTER TABLE {s}.kmers RENAME COLUMN count TO {c}".format(s=s, c=new_col_name))

    # Verify correct column names
    cursor.execute("SELECT * FROM {}.kmers".format(s))
    col_names = [tuple[0] for tuple in cursor.description]
    assert "count1" in col_names or "count2" in col_names, "Unrecognized column names: {}".format(str(col_names))

# Create new tables
cursor.execute("DROP TABLE IF EXISTS compare_kmers")
cursor.execute("CREATE TABLE compare_kmers \
                (seq TEXT PRIMARY KEY UNIQUE, \
                count1 INTEGER DEFAULT 1, \
                count2 INTEGER DEFAULT 1)")

# Join content from previous tables into new table
sys.stderr.write("Inserting kmers from both databases into new table\t\t{}\n".format(ctime()))

cursor.execute("INSERT INTO compare_kmers \
                SELECT sample1.kmers.seq, \
                    sample1.kmers.count1, \
                    CASE WHEN sample2.kmers.count2 IS NULL THEN 1 ELSE sample2.kmers.count2 END AS count2 \
                FROM sample1.kmers \
                LEFT JOIN sample2.kmers USING(seq) \
                UNION ALL \
                SELECT sample2.kmers.seq, \
                    CASE WHEN sample1.kmers.count1 IS NULL THEN 1 ELSE sample1.kmers.count1 END AS count1, \
                    sample2.kmers.count2 \
                FROM sample2.kmers \
                LEFT JOIN sample1.kmers USING(seq) WHERE sample1.kmers.count1 IS NULL")

# Print
# cursor.execute("SELECT * FROM compare_kmers")
# rows = cursor.fetchall()
# for row in rows:
#     print(row)


# Commit changes
sys.stderr.write("Committing changes to database, this may take some time\t\t{}\n".format(ctime()))
connection.commit()

sys.stderr.write("Done populating database {}\t\t{}\n".format(files[2], ctime()))
