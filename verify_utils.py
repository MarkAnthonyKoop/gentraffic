from koop_db import get_records_from_dbf, get_cut
from re import sub
import sys
import shutil

def getdates(ascii_traffic):
    return [''.join( (x.split('/')[0].zfill(2) , x.split('/')[1].zfill(2)) ) for x in ascii_traffic[2][2:]]

             
#verify all plays in the traffic log are played in the autofile
def test_playlist(traffic, playlist):
    for ti,t in enumerate(traffic):
        #print("looking for traffic entry",t)
        found = False
        for pi,p in enumerate(playlist):
            if p in t  or t in p:
                #print("found",p)
                found = True
                del playlist[pi]
                break
        if not found:
            print("WARNING --",t,"NOT FOUND IN PLAYLIST")

#organize the traffic by day
def get_weeklytraffic(ascii_traffic, dates):
    del ascii_traffic[:6]
    weekly_traffic = {x:[] for x in dates}
    for row in ascii_traffic:
        for i,date in enumerate(dates):
            weekly_traffic[date].append(row[i+2])
    return weekly_traffic

def get_playlist(date, cut_records, on_prod):
    if on_prod:
        dbf_file = 'K:/DAD/Files/PLAYLIST/'+date+'AUTO.DBF'
    else:
        dbf_file = 'autofiles/' + date + 'AUTO.DBF'
    try: records = get_records_from_dbf(dbf_file)    
    except: return None
    if on_prod:
        shutil.copy(dbf_file,'autofiles')
    start = False
    playlist = []
    for record in records:
        if not start:
            if 'DELAY' in record['CUT']: 
                start = True
            continue
        if 'Local Music Mix' in record['COMMENT']:
            break
        if not record['CUT'].isnumeric():
            continue
        cutid = record['CUT']
        cut = get_cut(cutid,cut_records)
        if record['FUNCTION'] == 'L' and record['CUT'][0:2] != '99' and cut:
            playlist.append( cut['TITLE'] )
            if (float(cut['LENGTH']) > 232 and 
                    cut['CUT'] != '12260'  and
                    cut['CUT'] != '00605'):
                print("ERROR!!!!!!!!!!!!! Long cut:",
                       cutid," length: ",cut['LENGTH'] )
                sys.exit("Long Cut")
    return playlist

