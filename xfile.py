from koop_db import get_records_from_dbf, get_cut, mylog
from re import search
import shutil
from glob import glob

def mysort(val):
    sort_string = search(r"\[.*?,.*?,.*?(.*?)\)",val[1])
    sort_number = float(sort_string[1])
    return sort_number


def xfile():
    lines = []

    for file in glob("logs/*xref*"):
        with open(file) as f:
            lines.extend(f.readlines())

    requested = lines[::2]
    found  = lines[1::2]


    zippy = list(zip(requested,found))
    zippy.sort(key=mysort)

    (new_requested, new_found) = list(zip(*zippy))

    new_lines = [None] * len(lines)
    new_lines[::2] = new_requested
    new_lines[1::2] = new_found

    mylog(new_lines,"xfile.txt")

if __name__ == "__main__":
    xfile()
