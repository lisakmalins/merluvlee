# Lisa Malins
# 2 Oct 2019
# SortKmers.py

import sys
from os import stat
import gc
from datetime import timedelta
from time import process_time
from time import ctime

class kmer:
    def __init__(self, seq, count):
        self.seq = seq
        self.count = count

def sort_as_list(oligos):
    output = open("listsort.fa", 'w')
    log = open("listsort.log", 'w')
    log.write("Begin reading into list at: {}\n".format(ctime()))

    kl = list()

    oligos.seek(0)
    line = oligos.readline()

    # Setup 10% progress counter
    filelength = float(stat(oligos.name).st_size)
    begin_time = process_time()
    last_check = begin_time
    percent = 10

    while line:
        # 10% progress counter
        if (oligos.tell() / filelength * 100) >= percent:
            current_check = process_time()
            print(percent, "percent completed")
            # print(str(timedelta(seconds=current_check - last_check)))
            log.write("{} percent read at {}\n".format(str(percent), ctime()))
            log.write("\tDiff: {}\n".format(str(timedelta(seconds=current_check - last_check))))
            log.flush()
            last_check = current_check
            percent += 10

        # Get sequence & count
        assert line[0] == ">", "Expected fasta header, instead saw " + str(line)
        count = line[1:-1]
        # print(count)
        seq = oligos.readline().rstrip()

        k = kmer(seq, count)
        kl.append(k)

        line = oligos.readline()

    begin_sort = process_time()
    log.write("Begin sorting list at: {}\n".format(ctime()))
    log.flush()

    # Sort as list
    kl.sort(key = lambda item: item.seq)

    end_sort = process_time()
    log.write("End sorting list at: {}\n".format(ctime()))
    log.write("List sort time: {}\n".format(str(timedelta(seconds=end_sort-begin_sort))))
    log.flush()

    # Write sorted list
    for entry in kl:
        # print(entry.seq, entry.count)
        output.write(entry.seq)
        output.write(" ")
        output.write(str(entry.count))
        output.write("\n")

    log.write("Total time: {}\n".format(str(timedelta(seconds=process_time()-begin_time))))

def sort_as_dict(oligos):
    output = open("dictsort.fa", 'w')
    log = open("dictsort.log", 'w')
    log.write("Begin reading into dict at: {}\n".format(ctime()))

    kd = {"": 0}

    oligos.seek(0)
    line = oligos.readline()

    # Setup 10% progress counter
    filelength = float(stat(oligos.name).st_size)
    begin_time = process_time()
    last_check = begin_time
    percent = 10

    while line:
        # 10% progress counter
        if (oligos.tell() / filelength * 100) >= percent:
            current_check = process_time()
            print(percent, "percent completed")
            # print(str(timedelta(seconds=current_check - last_check)))
            log.write("{} percent read at {}\n".format(str(percent), ctime()))
            log.write("\tDiff: {}\n".format(str(timedelta(seconds=current_check - last_check))))
            log.flush()
            last_check = current_check
            percent += 10

        # Get sequence & count
        assert line[0] == ">", "Expected fasta header, instead saw " + str(line)
        count = line[1:-1]
        # print(count)
        seq = oligos.readline().rstrip()

        kd[seq] = count

        line = oligos.readline()

    # Remove dummy entry
    kd.pop("")

    # for entry in kd.items():
    #     print(entry)

    begin_sort = process_time()
    log.write("Begin sorting dict at: {}\n".format(ctime()))
    log.flush()

    # Sort dictionary items & save as list
    sorted_kd = sorted(kd.items())
    end_sort = process_time()
    log.write("End sorting dict at: {}\n".format(ctime()))
    log.write("Dict sort time: {}\n".format(str(timedelta(seconds=end_sort-begin_sort))))
    log.flush()

    # Write all
    for entry in sorted_kd:
        output.write(entry[0])
        output.write(" ")
        output.write(str(entry[1]))
        output.write("\n")

    log.write("Total time: {}\n".format(str(timedelta(seconds=process_time()-begin_time))))

#TODO die politely
try:
    oligos = open(sys.argv[1], 'r')
except:
    oligos = open("microdump.fa", 'r')

print("Reading oligos from", oligos.name)

starttime = process_time()
sort_as_list(oligos)
endtime = process_time()
sort_as_list_time = endtime - starttime
print("Sort as list time:\t", str(timedelta(seconds=sort_as_list_time)))

starttime = process_time()
sort_as_dict(oligos)
endtime = process_time()
sort_as_dict_time = endtime - starttime
print("Sort as dict time:\t", str(timedelta(seconds=sort_as_dict_time)))
