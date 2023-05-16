verbose = False 
logger  = True 
import sys
import csv
import shutil
import random
import difflib
import copy
import dbf
import re

from sys import argv
from zipfile import *
from difflib import SequenceMatcher
from pprint import pprint
from heapq import nlargest as _nlargest
from dbfread import DBF
from itertools import repeat
from koop_db import *
from os.path import exists
from xfile import xfile
from update_dad import update_dad
from verify_results import verify_results

startday = "MONDAY"

if len(argv) > 1:
    startday = argv[1].upper()

if "MON" not in startday.upper() and startday.upper() not in "MONDAY":
    print("\n\nWarning--date check overriddden!\n",
    "\t(date checking is bypassed whenever startday is not Monday)\n\n")

print("\nStarting on",startday)

on_prod = False

if exists('K:/DAD/Files/CUTS.DBF'):
    on_prod = True

def gen_auto(playout, day):
    filename = "outputfiles/" + day.upper()[:8] + ".DBF"
    auto_table = dbf.Table('inputfiles/auto.dbf')
    auto_table.open()
    new_auto_table = auto_table.new(filename)
    new_auto_table.open(dbf.READ_WRITE)
    new_auto_fields = get_internal_fields()
    new_auto_fields.remove('TITLE') 
    #new_auto_fields.remove('TIME') 
    #new_auto_fields.remove('COMMENT') 
    #print("auto_fields is",new_auto_fields)
    new_playout = prune_records(playout, new_auto_fields)
    for row in new_playout:
        #print("row is:",row)
        new_auto_table.append(row)
    new_auto_table.close()
    auto_table.close()
    #new_auto_table_from_disk = dbf.Table('outputfiles/new_auto.dbf')
    #new_auto_table_from_disk.open()
    #new_auto_records = []
    #for row in new_auto_table_from_disk:
    #    print(row)
    #    new_auto_records.append(row)
    #new_auto_table_from_disk.close()
    #gentraffic_ascii = records2ascii(new_auto_records)
    #gentraffic_ascii.insert(0,create_file_format_header())
    #mylog(gentraffic_ascii, filename)
    #print("\n\n\n\nAnd here is the lovely output:\n")
    #print(gentraffic_ascii)
    return None

def create_song_list(cut_records):
    songs = []
    legal_ids = []
    for cut_record in cut_records:   
        cut_id = cut_int(cut_record['CUT'])
        userdef = cut_record['USERDEF']
        if userdef == 'Local' or userdef == 'Local Artist' or userdef == 'local':
            song_record = copy.deepcopy(cut_record)
            song_record['FUNCTION'] = 'A'
            song_record['TYPE'] = 'P'
            songs.append(song_record)
        if cut_id > 99942 and cut_id < 99953:
            song_record = copy.deepcopy(cut_record)
            song_record['FUNCTION'] = 'A'
            song_record['TYPE'] = 'P'
            legal_ids.append(song_record)
    mylog(cut_records, 'cut_records.log')
    mylog(songs, 'songs.log')
    mylog(legal_ids, 'legal_ids.log')
    if(verbose):
        print("Creating random song list...")
    num_songs = len(songs);
    random_indexes = random.sample(range(0,num_songs),700)
    some_random_songs = []
    for i in random_indexes:
        #print("i is ",i)
        some_random_songs.append(songs[i])
    mylog(some_random_songs, 'some_random_songs.log')
    if verbose:
        print("Creating random legal id list...")
    num_ids = len(legal_ids);
    random_indexes = random.choices(range(0,num_ids),k=40)
    some_random_legal_ids = []
    for i in random_indexes:
        #print("i is",i)
        some_random_legal_ids.append(legal_ids[i])
    mylog(some_random_legal_ids, 'some_random_legal_ids.log')
    #print("random song depth is ",len(some_random_songs))
    return (some_random_songs, some_random_legal_ids)

def convert_traffic_old(traffic_data): #todo--add cut field to traffic here so we only have to go through cut_records once
    day_lookup = {'Monday': 4, 'Tuesday' : 6, 'Wednesday' : 8, 'Thursday' : 10, 'Friday' : 12, 'Saturday' : 14, 'Sunday' : 16} 
    traffic = {}
    for day in day_lookup:
        day_index = day_lookup[day]
        daily_traffic = []
        for row in traffic_data:
            if verbose:
                print("full row is:",row)
                print("row[7] ==>", row[7])
                print("row[8] ==>", row[8])
                print("row[9] ==>", row[9])
            if row[1] == '':
                time = row[0]
            else:
                time = row[1]
            if row[day_index] == '':
                title = row[day_index+1]
            else:
                title = row[day_index]
            if title == "RUN EAS":
                title = "EAS TEST"
            daily_traffic.append({'TIME':time,'TITLE':title,'TYPE':'P'})
        traffic[day] = daily_traffic
        #print("traffic[day] is",traffic[day])
    mylog(traffic, 'traffic.log')
    return traffic

#converts list of strings to a list of records
def convert_traffic(traffic_data): #todo--add cut field to traffic here so we only have to go through cut_records once
    traffic = {}
    for i,day in enumerate(weekdays()):
        daily_traffic = []
        for row in traffic_data:
            time = row[1]
            if time and not time.isspace():
                time = traffic_get_seconds(time)
                time = make_time_str(time)
            title = row[i+2]
            #print("full row is:",row)
            #print("row[4] ==>", row[4])
            if title.strip().lower() == "run eas":
                title = "EAS Test"
            if ('EOB FM' not in title and 'EOBD FM' not in title
                                      and 'SOB FM' not in title
                                      and 'SOBD FM' not in title):
                daily_traffic.append({'TIME':time,'TITLE':title})
        traffic[day] = daily_traffic
        daily_traffic_summary = [x for x in daily_traffic if x['TITLE'] ]
        daily_traffic_ascii = [x['TIME']+'---> '+x['TITLE'] if x['TIME'] else '              '+x['TITLE'] for x in daily_traffic_summary]
        mylog(daily_traffic_ascii,day+"_traffic.log")
    mylog(traffic, 'traffic.log')
    return traffic


#lookup the 'CUT" number from cut records that most closely
#matches 'TITLE' in traffic_records, and add a 'CUT' field
#to all the traffic records associated with that 'TITLE'
def xref_traffic_titles(traffic_records, cut_records):
    cut_titles = [t['TITLE'].lower() for t in cut_records]
    #pprint(cut_titles)
    mylog(cut_titles,"cut_titles.log")
    unique_titles = {}
    for day in traffic_records:   #produce a dictionary with requested titles as a key 
        warnings = []             #and a value of [cut number,cut title]  
        xref_log_data = []
        for row in traffic_records[day]:
            title = munge_traffic_title(dd(row,'TITLE'))
            if (title != '' and title not in unique_titles.keys() 
                            and title != 'sign on'):
                #print("xref looking for:",title)
                match_index = get_close_matches_indexes(title, cut_titles, cutoff=0.5)
                #print("match_index is:",match_index)
                mi = match_index[0][0]
                unique_titles[title] = [ cut_records[mi]['CUT'], cut_titles[mi] ]
                xref_log_data.append("requested: " + title.ljust(32)[:32] + "\n" +
                                     "    found: " + cut_titles[mi].ljust(32)[:32] + 
                                     "-->" + str(match_index) )
                if match_index[0][2] < .85:
                    if verbose:
                        print("\n  !!!!!!!!!!!!!!!WARNING!!!!!!!!!!!!!!!!!")
                        print("      ----- Match Score below .85 -----\n")
                        print(" traffic title requested:  ",title)
                        print(" closest cut title found:  ",cut_titles[mi])
                        print("\n match index:\n")
                        pprint(match_index)
                        print("\n")
                    else:
                        print("Warning log/lib mismatch:  ",title," - ",cut_titles[mi])
                    warnings.append("requested: " + title),
                    warnings.append("    found: " + cut_titles[mi]),
        mylog(warnings,day+"_warnings.log")
        mylog(xref_log_data,day+"_xref.log")
    mylog(unique_titles,"weekly_titles.log")
    for day in traffic_records:
        for row in traffic_records[day]:
            #print("row is",row)
            title = munge_traffic_title(dd(row,'TITLE'))
            #print("title is",title)
            if title and title != 'sign on':
                row['CUT'] = unique_titles[title][0]
                #print("newcut is",row['CUT'])
                row['TYPE'] = 'P'
                row['FUNCTION'] = 'L'
                row['TITLE'] = unique_titles[title][1]
                #note that unique_titles['TITLE'][0] ==> cut_records['CUT']
                #(unique_titles was set up to associate titles w/cut ids)
    return None

def get_next_traffic_stack(traffic,current_time,traffic_function):
    traffic_stack = []
    traffic_time = 0
    traffic_row = {}
    while traffic_time < current_time:
        try:
            traffic_row = traffic.pop(0)
            #print("popping from traffic",traffic_row)
        except:
            #print("no more traffic--breaking")
            break
        traffic_time_string = dd(traffic_row,'TIME')
        if traffic_time_string != '':
            traffic_time = get_seconds(traffic_time_string)
        traffic_title     = dd(traffic_row,'TITLE')
        if (traffic_title != '' 
                and traffic_title != 'SIGN ON' 
                and traffic_time < current_time):
            #print("pushing traffic onto stack",traffic_row)
            traffic_row['FUNCTION'] = traffic_function
            traffic_stack.append(traffic_row)
            #print("traffic stack is ",make_time_str(traffic_time), 
            #      traffic_stack)
    if traffic_time >= current_time and traffic_row:
        traffic.insert(0,traffic_row) #queue this row back up for next stack
 
    return traffic_stack

#Add the weekly traffic from Melinda's pdf to the day's playlist
def add_traffic_to_playlist(playlist, traffic):
    if verbose:
        print("adding Melinda's traffic titles to template:")
    new_records = []
    current_time =  0
    traffic_time =  0
    traffic_function = 'L'
    for i,row in enumerate(playlist):
        time = dd(row,'TIME')
        time = re.sub("\s*","",time)
        cut = dd(row,'CUT')
        title = dd(row,'TITLE')
        #print("row is",row)
        if time != '':
            #print("time is--"+time+"--")
            current_time = get_seconds(time)
            #print("updating time to",current_time )
        if cut == 'TRFFC':
            traffic_function = dd(row,'FUNCTION')
            if not traffic_function:
                traffic_function = 'L'
        traffic_stack = get_next_traffic_stack(traffic, current_time, 
                                               traffic_function)
        for trow in traffic_stack:
            #print("inserting new traffic:",trow)
            new_records.append(trow)
        if cut != 'TRFFC':
            new_records.append(row)
            #print("adding row:",row)
    playlist.clear()
    playlist.extend(new_records) 
    return new_records

def add_prefix(playlist, time):
#summary:
#COMMENT -->  09:00:00         C -9 AM LEGAL ID Wednesday 169.09 
#HARD BR -->  09:00:00 HARD A T -This cut has no title! 169.09 
#        -->  09:00:00 DELAY A D -This cut has no title! 169.09 
#file format:
#['CUT',5], ['FUNCTION', 1],['DELAY',8],['PLAYS',2],['SEC',1],['TER',1],['SEGUE',1],['TIME',8],['BEGEND',1],['CHAIN',8],['ROTATE',8],['TYPE',1],['COMMENT',35],['LINEID',10],['STARTTIME',7],['ENDTIME',7],['FOSTART',7],['FOLENGTH',7],['FISTART',7],['FILENGTH',7],['LIBLOC',2],['LIBNAME',8],['GUID',36],['ORDERID',5]
#CUT  FDELAY   PLSTSTIME    BCHAIN   ROTATE  TCOMMENT
#HARD A0            09:00:0020       0       T
#DELAYA1.00         09:00:00                 D
#internal format: 
#['TIME','CUT','COMMENT','FUNCTION','TITLE', 'DELAY','BEGEND','CHAIN','ROTATE','TYPE','STARTTIME','ENDTIME','SEC','TER','SEGUE']

    comment = 'KOOP Broadcasting on ==> 91.7FM '

#              TIME, CUT  ,COMMENT,FUNC,TITLE,DELAY,BGND,CHAIN    ,ROTATE,TYPE,STARTTIME,ENDTIME,TITLE,SEC,TER,SEGUE
    prefix = [[time,''    ,comment,''  ,''   , ''  , '' ,''       ,''    ,'C'],
              [time,'HARD', ''    ,'A' ,''   ,'0'  ,'2' ,''       ,''    ,'T' ,'0.00'   ,'0.00'],
              [time,'DELAY',''    ,'A' ,''   ,'1.0',''  ,''       ,''    ,'D']]

    dicts = convert_lists_to_dicts(prefix)
    prepend(playlist,dicts)
    return None

def add_postfix(playlist, time):
#summary:
#CUT  FDELAY   PLSTSTIME    BCHAIN   ROTATE  TCOMMENT
#HARD A0            23:59:0020       0       T                                             0.00   0.00                                                                                  
#CHAINA            023:59:00HMMDDxxxx1       H                                             0.00   0.00                                                                                  
#internal format: 
#['TIME','CUT','COMMENT','FUNCTION','TITLE','DELAY','BEGEND','CHAIN','ROTATE','TYPE','STARTTIME','ENDTIME','SEC','TER','SEGUE']
#              TIME, CUT  ,COMMENT,FUNC,TITLE,DELAY,BGND,CHAIN    ,ROTATE,TYPE ,STARTTIME,ENDTIME,SEC,TER,SEGUE
    postfix = [[time,'HARD', ''    ,'A' ,''   ,'0'  ,'2' ,''        ,''    ,'T' ,'0.00'   ,'0.00'],
              [time,'CHAIN',''    ,'A' ,''   ,'1.0','H' ,'MMDDxxxx','1'   ,'H' ,'0.00'   ,'0.00']]

    dicts = convert_lists_to_dicts(postfix)
    playlist.extend(dicts)
    return None

def get_start_stop_times(playlist):
    mylog(playlist,"start_stop.log1")
    start_time = "00:00:00" 
    stop_time = "23:59:59"
    #print("entering get_start_stop_time")
    for rec in playlist:
        #print("rec is",rec)
        #print("time is",rec['TIME'])
        if ':' in rec['TIME']:
            start_time = rec['TIME']
            break
    #print("midpoint get_start_stop_time")
    mylog(playlist,"start_stop.log2")
    for rec in playlist:
        #print("rec is",rec)
        #print("time is",rec['TIME'])
        if ':' in rec['TIME']:
            stop_time = rec['TIME']
    #print("start_time",start_time)
    #print("stop_time",stop_time)
    #print("xiting get_start_stop_time")
    mylog(playlist,"start_stop.log3")
    return (start_time, stop_time)
     
def create_list_of_ordered_cuts(start_time, end_time, cut_list):
    (some_random_songs,some_random_legal_ids) = cut_list
    hour_threshold = 3541
    new_cuts = []
    overflow = []
    seconds = get_seconds(start_time)
    start_hour = int(seconds/3600)
    end_hour = int(end_time[0:2])
    seconds = seconds - start_hour*3600
    hour_range = range(start_hour, end_hour)
    for hour in hour_range:
        in_current_hour = True 
        some_random_songs.extend(overflow)
        overflow = []
        while  in_current_hour:
            #print("random song depth is ",len(some_random_songs))
            if len(some_random_songs) < 2:
                some_random_songs.extend(overflow)
                overflow = []
                offending_cut = new_cuts.pop()
                some_random_songs.insert(0,offending_cut)
                seconds -= float(offending_cut['LENGTH'])
            next_song = copy.deepcopy(some_random_songs.pop())
            length = float(next_song['LENGTH'])
            #print("seconds is", format(seconds,".2f"),
            #        "length is", format(length,".2f"),
            #        "sum is ", format(seconds + length,".2f"),
            #        "current hour is", hour,
            #        "hour_threshold is", hour_threshold,
            #        "in_current_hour is", in_current_hour)
            if  seconds + length < hour_threshold: 
                seconds += length
                new_cuts.append(next_song); #print("appending song:",next_song)
                #print("<thresh seconds is", format(seconds,".2f"))
            elif seconds + length > 3600:
                overflow.append(next_song); #print("overflow appending:",next_song['CUT'])
                #print(">3600   seconds is", format(seconds,".2f"))
            elif seconds + length > hour_threshold:
                #print(">thresh seconds is", format(seconds,".2f"))
                seconds += length
                new_cuts.append(next_song); #print("appending song:",next_song)
                if hour != end_hour - 1:
                    random_legal_id = copy.deepcopy(some_random_legal_ids.pop())
                    new_cuts.append(random_legal_id); #print("appending legal id:",random_legal_id)
                    #print("appending legal id branchy",hour,make_time_str(seconds),seconds,random_legal_id)
                    seconds += float(random_legal_id['LENGTH'])
                #print(">thresh second2 is", format(seconds,".2f"))
                seconds -= 3600
                #print(">thresh second3 is", format(seconds,".2f"))
                in_current_hour = False
    while seconds < 400:  #pad before hard branch
       next_song = copy.deepcopy(some_random_songs.pop())
       new_cuts.append(next_song); #print("appending extra song",next_song)
       seconds += float(next_song['LENGTH'])
    new_cuts.insert(0,copy.deepcopy( some_random_legal_ids.pop() ) )
    return new_cuts

def add_overnight_cuts(playlist, cut_list):
    if verbose:
        print("adding overnight cuts")
    (start_live_time, end_live_time) = get_start_stop_times(playlist)
    add_prefix(playlist,start_live_time)
    morning_music = create_list_of_ordered_cuts('00:00:00',start_live_time, cut_list)
    evening_music = create_list_of_ordered_cuts(end_live_time,'24:00:00', cut_list)
    update_times(0,morning_music)
    update_times(get_seconds(end_live_time),evening_music)
    prepend(playlist,morning_music)
    playlist.extend(evening_music)
    add_postfix(playlist,'23:59:59')
    return None

def update_times(start_time, records):
    seconds = start_time
    for rec in records:
        rec['TIME'] = make_time_str(seconds)
        length = dd(rec,'LENGTH')
        seconds += float(length)
    return None

def summarize_playlist(playlist):
    if(verbose):
        print("creating summary for playlist")
    summary_records = []
    for cut in playlist:
        summary_record = ljust_record(cut)
        #print("cut is",cut)
        mytype = '        -->  '
        cut_str  = dd(cut,'CUT')
        #print("cut string is ",cut_str)
        title_str  = dd(cut,'TITLE')
        #print("yyyxxx dd(cut,'COMMENT') is",dd(cut,'COMMENT'),type(dd(cut,'COMMENT')) )
        comment_str = dd(cut,'COMMENT')
        if comment_str.isspace() or not comment_str: 
            #print("no comment")
            if title_str.isspace() or not title_str:
                summary_record['COMMENT'] = "            no comment                "
            else:
                summary_record['COMMENT'] = title_str
        if cut_str[0:2] == '99':
            mytype = 'LEGALID -->  '
        if cut_str == '':
            mytype = 'COMMENT -->  '
        if cut_str == '00666':
            mytype = 'EAS TST -->  '
        if cut_str == 'HARD':
            mytype = 'HARD BR -->  '
        if cut_str == 'CHAIN':
            mytype = 'CHAINTO -->  '
        #print("titlis:",dd(summary_record,'TITLE') )
        #print("summary_record['COMMENT'] is:",summary_record['COMMENT'])
        summary_record['MYTYPE'] = mytype
        summary_records.append(summary_record)
    summary = []
    for cut in summary_records:
        astring = (dd(cut,'MYTYPE') 
                 + dd(cut,'TIME').ljust(8) + " " 
                 + dd(cut,'COMMENT').ljust(35)[:35] + " " 
                 + dd(cut,'CUT').ljust(5)[:5] 
                 + dd(cut,'FUNCTION').ljust(1)[:1]  
                 + dd(cut,'TYPE').ljust(1)[:1] + " "
                 + dd(cut,'LENGTH').rjust(8)[:8] 
                  ) 
        #print("astring is ",astring)#(and by any other name, still a string)
        summary.append(astring)
    return summary
   
def gen_playout_log(playout, day):
    filename = day.lower() + "_playout.log"
    asci = records2ascii(playout)
    #uncomment this line to see the header in the output
    #asci.insert(0,create_file_format_header())
    if verbose:
        print("generating",filename)
    mylog(asci, filename)
    shutil.copy('logs/'+filename, 'outputfiles/'+day.upper()[:8]+'.LOG')
    if on_prod:
        shutil.copy('logs/'+filename, 'K:/DAD/Import/'+day.upper()[:8]+'.LOG')
    #print("\n\n\n\nAnd here is the lovely output:\n")
    #print(gentraffic_ascii)

def gen_1_day_of_traffic(playlist, day, traffic, cut_records):
    overnight_cuts = create_song_list(cut_records)
    mylog(playlist,day+"_playlist_b4_traffic.log")
    add_traffic_to_playlist(playlist, traffic)
    mylog(playlist,day+"_trafficted_playlist.log")
    add_overnight_cuts(playlist, overnight_cuts)
    mylog(playlist,day+"_playlist.log")
    mylog(summarize_playlist(playlist),day+"_summary.log")
    gen_playout_log(playlist, day)
    gen_auto(playlist, day)
    return None

def gentraffic():
    print("\ngentraffic v1.02b\n")
    print("  last updated on: 3/21/23")
    print("  last updated by: Markanth\n\n")
    print("generating traffic...\n")
    if verbose:
        print("active working directory is",os.getcwd())
    if on_prod:
        cut_records = get_records_from_dbf('K:/DAD/Files/CUTS.DBF',ignore_archived=True)
    else:
        cut_records = get_records_from_dbf('./inputfiles/cuts.dbf',ignore_archived=True)

    templates = read_templates()
    playlists = convert_templates(templates)
    mylog(playlists,'playlists.log')
    mylistlog(playlists,'template_conversion.log')
    ascii_traffic = read_traffic(clip_top=True)
    mylog(ascii_traffic,"ascii_traffic.log")
    traffic = convert_traffic(ascii_traffic)
    mylog(traffic,"traffic_conversion.log")
    xref_traffic_titles(traffic, cut_records)
    mylog(traffic,"traffic_x_ref.log")
    #use this line to generate the full week of playout
    list(map(gen_1_day_of_traffic, playlists, weekdays()
            ,traffic.values(), repeat(cut_records)))
    #use this line to generate just one specific day of playout
    #list(map(gen_1_day_of_traffic, [playlists[2]], ['Wednesday'] 
    #        ,repeat(traffic['Wednesday']), repeat(cut_records)))
    print("\nGentraffic Finished!!!!-Traffic Successfully Generated for",startday,"to Sunday!\n\n")
    return True

if gentraffic():
    xfile()
    print("updating DAD with startday",startday)
    update_dad(startday)
    verify_results()
    print("\nAll Programs Finished!!\n")
    
#zipfile.ZipFile('

