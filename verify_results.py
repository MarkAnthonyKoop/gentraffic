from os.path import exists
from os  import chdir
from koop_db import get_records_from_dbf, read_traffic, list2file, mylower
from verify_utils import getdates, test_playlist, get_weeklytraffic, get_playlist
from difflib import ndiff
from subprocess import call
import shutil


def verify_results():
    on_prod = False
    if exists('K:/DAD/Files/CUTS.DBF'):
        on_prod = True
    
    if on_prod:
        cut_records = get_records_from_dbf('K:/DAD/Files/CUTS.DBF')
        shutil.copy('K:/DAD/Files/CUTS.DBF','inputfiles/cuts.dbf')
    else:
        cut_records = get_records_from_dbf('inputfiles/cuts.dbf')
    
    ascii_traffic = read_traffic()
    weekof = ascii_traffic[1][2]
    
    print("VerifyResults now running and verifying traffic for week of:",weekof,"\n")
    dates = getdates(ascii_traffic)
    #print("dates are:",dates,'\n')
    
    weekly_traffic = get_weeklytraffic(ascii_traffic,dates)
    
    #import the AUTO files and verify traffic
    for date in dates:
        playlist = get_playlist(date, cut_records, on_prod)
        if not playlist:
            continue
        traffic = weekly_traffic[date]
        mylower(traffic)
        mylower(playlist)
        ndiffs = ndiff(playlist,traffic)
        #print("\nVerifying -->",date,":\n")
        #print("--------------differences------------")
        #print('\n'.join(ndiffs))
        #print("-----------Requested Traffic---------")
        #print('\n'.join(traffic))
        #print("-------------Actual Traffic----------")
        #print('\n'.join(playlist))
        #print("-------------test results------------")
        playlist_file = "logs/"+date+"playlist.txt"
        traffic_file  = "logs/"+date+"traffic.txt"
        list2file(playlist,playlist_file)
        list2file(traffic,traffic_file)
        test_playlist(traffic, playlist)
        call(['C:/program_files/vim/vim90/gvim', '-d',traffic_file,playlist_file])
    shutil.make_archive('../gt'+dates[0], 'zip', '..','gentraffic')
    return True


if __name__ == "__main__":
    verify_results()


