from os.path import exists
from os  import chdir
from koop_db import get_records_from_dbf, read_traffic, list2file, mylower
from verify_utils import getdates, test_playlist, get_weeklytraffic, get_playlist
from difflib import ndiff
from subprocess import call
import shutil


def log_autos():
    ascii_traffic = read_traffic()
    dates = getdates(ascii_traffic)
    print("dates are:",dates,'\n')
    
    #import the AUTO files and verify traffic
    for date in dates:
        dbf_file = 'K:/DAD/Files/PLAYLIST/'+date+'AUTO.DBF'

        records = get_records_from_dbf(dbf_file)

        print("\n\n\n\n",date+"AUTO\n\n\n\n")
        for record in records:
            print(record)


    return True

log_autos()
