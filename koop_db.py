import os
import sys
import csv
import glob
import shutil
import random
import difflib
import copy
import dbf

from itertools import repeat
from difflib import SequenceMatcher
from dbfread import DBF
from pprint import pprint
from heapq import nlargest as _nlargest
from re import sub

from os.path import exists

verbose = False
logger = True

def enumerated_product(*args):
    yield from zip(product(*(range(len(x)) for x in args)), product(*args))

def get_seconds(time_str):
    """Get seconds from time."""
    h, m, s = time_str.split(':')
    return float(int(h) * 3600 + int(m) * 60 + int(s))

def hour(seconds):
    """Get hour as float from seconds."""
    afloat = seconds/3600
    return afloat

def get_len(cut_str, cut_records):
    for cut in cut_records:
        if cut_int(cut_str) == cut_int(cut['CUT']):
            return cut['LENGTH']
    print("Error!!!!!   cut:",cut_str,"not found")

def get_cut(cut_str, cut_records):
    for cut in cut_records:
        if cut_int(cut_str) == cut_int(cut['CUT']):
            return cut
    print("Error!!!!!   cut:",cut_str,"not found")
    return False 

def make_time_str(seconds):
    """Create time string from seconds"""
    hour = int(seconds / 3600)
    minute = int((seconds - hour * 3600)/60)
    second = int(seconds - (hour * 3600 + minute * 60))
    time_str = str(hour).zfill(2) + ':' + str(minute).zfill(2) + ':' + str(second).zfill(2)
    return time_str

#def dd(adict, astring):
#    return adict[astring] if astring in adict.keys() else ''

def dd(adict, astring):
    if astring in adict.keys():
        return adict[astring]
    else:
        return ''

def dict_deref_space_reduce(adict, astring):
    avalue = ''
    if astring in adict.keys():
        avalue = adict[astring]
    return '' if avalue.isspace() else avalue
 
#todo--just for kicks figure out which if test is evaluated first
def dd_experimental(d, s):
    return d[s] if s in d.keys() else '' if d[s].issspace() else ''
     
def traffic_get_seconds(astring):
    newstring = sub("[A-z]","",astring)
    newstring = sub("\s","",newstring)
    hour,minute = newstring.split(':')
    noon = hour == '12'
    try:
        seconds = int(hour) * 3600 + int(minute) * 60
    except:
        seconds = 0
    #print("noon is",noon)
    offset = 0
    if "PM" in astring and not noon:
        offset = 12 * 3600
    if "pm" in astring and not noon:
        offset = 12 * 3600
    #print("traffic_get_seconds: ",newstring,hour, minute, seconds, offset)
    return seconds + offset
 
def time_int(astring):
    newstring = sub("\D","",astring)
    culled = newstring[0:4]
    #print("culled is",culled)
    noon = culled[0:2] == '12'
    #print("noon is",noon)
    offset = 0
    if "PM" in astring and not noon:
        offset = 1200
    if "pm" in astring and not noon:
        offset = 1200
    #print("time int: ",astring,newstring,culled,int(culled),int(culled)+offset)
    return int(culled) + offset

def mylistlog(list_of_iterables, fname):
    if type(list_of_iterables) == dict:
        for i,astring in enumerate(list_of_iterables):
            mylog(list_of_iterables[astring],fname+i)
    else:
        for i,iterable in enumerate(list_of_iterables):
            mylog(iterable,fname+str(i))
    return None

#todo--add verbose option that uses Python's cool introspective 
#      skills to implement a message with calling functions 
#      followed by a nice pprint
def mylog(iterable, fname):
    #print("entering melog")
    if(logger):
        #print("entering logger")
        f = open('logs/'+fname.lower(), "w")
        for i in iterable:
            #print("i is",i)
            if type(iterable) is dict:
                #print("is dict")
                i = str(i) + ": " + str(iterable[i]) 
                #print("i is",i)
            astring = str(i) + "\n"
            f.write(astring)
            if verbose:
                print(i)
        return f.close()

def list2file(alist, fname):
    f = open(fname, "w")
    f.write("\n".join(alist))
    return None

def cut_int(astring):
    try: 
        return int(astring)
    except: 
        return -1

def prepend(file, list2insert):
    list(map(file.insert,repeat(0),reversed(list2insert)))
    return None

def get_close_matches_indexes(word, possibilities, n=3, cutoff=0.6):
    """Use SequenceMatcher to return list of the best "good enough" matches.

    word is a sequence for which close matches are desired (typically a
    string).

    possibilities is a list of sequences against which to match word
    (typically a list of strings).

    Optional arg n (default 3) is the maximum number of close matches to
    return.  n must be > 0.

    Optional arg cutoff (default 0.6) is a float in [0, 1].  Possibilities
    that don't score at least that similar to word are ignored.

    The best (no more than n) matches among the possibilities are returned
    in a list, sorted by similarity score, most similar first.

    >>> get_close_matches("appel", ["ape", "apple", "peach", "puppy"])
    ['apple', 'ape']
    >>> import keyword as _keyword
    >>> get_close_matches("wheel", _keyword.kwlist)
    ['while']
    >>> get_close_matches("Apple", _keyword.kwlist)
    []
    >>> get_close_matches("accept", _keyword.kwlist)
    ['except']
    """
    if not n >  0:
        raise ValueError("n must be > 0: %r" % (n,))
    if not 0.0 <= cutoff <= 1.0:
        raise ValueError("cutoff must be in [0.0, 1.0]: %r" % (cutoff,))
    result = []
    s = SequenceMatcher()
    s.set_seq2(word)
    for i,x in enumerate(possibilities):
        s.set_seq1(x)
        if s.real_quick_ratio() >= cutoff and \
           s.quick_ratio() >= cutoff and \
           s.ratio() >= cutoff:
            result.append((s.ratio(), i, x))
    # Move the best scorers to head of list
    result = _nlargest(n, result)
    # Strip scores for the best n matches
    return [(i,x,score) for score, i, x in result]

#The ASCII file format for DAD import is as shown in the list below
#  [Field name, field width in chars]  eg The first field is 
#  the 'CUT' field and it is 5 chars (or 5 8-bit bytes) wide 
#  todo--consider moving these comments to multi-line strings 
#        inside the function definitions as is 'the way' of PEPy
#        Pythonic Progression 
#        
def get_file_format():
    file_format = [['CUT',5], ['FUNCTION', 1],['DELAY',8],['PLAYS',2],['SEC',1],['TER',1],['SEGUE',1],['TIME',8],['BEGEND',1],['CHAIN',8],['ROTATE',8],['TYPE',1],['COMMENT',35],['LINEID',10],['STARTTIME',7],['ENDTIME',7],['FOSTART',7],['FOLENGTH',7],['FISTART',7],['FILENGTH',7],['LIBLOC',2],['LIBNAME',8],['GUID',36],['ORDERID',5]]
    return file_format

#similar to get_file_format, but includes 'skips' for fields not wanted for
#DAD import.   eg GUID should be generated by DAD and not included in the import
def get_output_fields():
    output_fields = [['CUT',5], ['FUNCTION', 1],['DELAY',8],['PLAYS',2],['SEC',1],['TER',1],['SEGUE',1],['TIME',8],['BEGEND',1],['CHAIN',8],['ROTATE',8],['TYPE',1],['COMMENT',35],['skip',10],['STARTTIME',7],['ENDTIME',7],['skip',7],['skip',7],['skip',7],['skip',7],['skip',2],['skip',8],['skip',36],['skip',5]]
    return output_fields

#returns the fields used in the internal format of this program
#these are ordered by most often used, so lists can be converted
#with as few items as possible to avoid a bunch of preceding empty
#strings and commas to avoid abominations like these: 
#      ['','','','','','','field of interest']
#also considered in the order are the asthetics.  eg time works well
#as the leftmost since the playlists are best displayed in chronologically
#todo--align dicts (which in Python 3.9 are ordered by default) to be in 
#      this order for all playlists' records
def get_internal_fields():
    internal_fields = ['TIME','CUT','COMMENT','FUNCTION','TITLE','DELAY','BEGEND','CHAIN','ROTATE','TYPE','STARTTIME','ENDTIME','SEC','TER','SEGUE']
    return internal_fields

def get_field_len(field):
    fields = get_output_fields()
    fields_dict = {field[0]: field[1] for field in fields}
    try: 
        length = fields_dict[field]
    except: 
        length = 0
    return length

#prune any records not listed in fields
def prune_records(records, fields = []):
    if fields == []:
        fields = get_internal_fields()
    new_records = []
    for rec in records:
        new_rec = {}
        for field in fields:
            new_rec[field] = dd(rec,field)
        new_records.append(new_rec)
    return new_records

def records2ascii(records):
    file_format = get_output_fields()
    ascii_records = []
    for rec in records:
        #print("rec is",rec)
        ascii_row = ''
        for field in file_format:
            #print("field is ",field)
            try:
                next_str = rec[field[0]]
            except:
                next_str = ''
            if field[0] == 'skip':
                next_str = ''
            #if len(next_str) > field[1]:
            #    print("\n\n\n\nError!!!!! records2ascii field",
            #            next_str,"is too long:",next_str,field[1])
            next_str = next_str.ljust(field[1])[:field[1]]
            #print("next_str is--",next_str,"--")
            ascii_row = ascii_row + next_str
        #print("appending--",ascii_row,"--")
        ascii_records.append(ascii_row)
    return ascii_records

def create_file_format_header():
    file_format = get_file_format()
    astring = ''
    for field in file_format:
        fstr = field[0]
        fstr_len = len(fstr)
        flen = field[1]
        if fstr_len > flen:
            astring = astring + fstr[0:flen]
        else:
            astring = astring + fstr.ljust(flen)
    return astring
 
def get_records_from_dbf(filename,ignore_archived=False):
    """This function returns a table representing the 
       dbf file specified by the argument 'filename'

       Input: filename
       Output: a list of dictionaries containing the 
               data in the dbf database """
       
    print("getting records from",filename)
    dbftable = DBF(filename,char_decode_errors='ignore')
    #print(dbftable)
    records = []
    for record in dbftable:
        if ignore_archived:
            if record['GROUP'] != 'ARCHIVED' and record['KILLDATE'].strip() == '':
                records.append(record)
        else:
            records.append(record)
    #print("\n\n\n\n",records)
    return records

def find_cut_from_cutid(cut,cut_records):
    found_record = {}
    #print("looking for ",cut)
    for cut_record in cut_records:
        if cut_int(cut_record['CUT']) == cut_int(cut):
            found_record = copy.deepcopy(cut_record)
            #print("found:",found_record['TITLE'])
            break
    return found_record

def cross_ref_cut_titles(records, cut_records):
    titled_records = []
    #print("in")
    for record in records:
        #print("trying record",record)
        try:
            #print("trying real hard")
            cut = record['CUT']
        except:
            #print("failed to find cut")
            titled_records.append(record)
            continue
        cut_record_copy = find_cut_from_cutid(cut,cut_records)
        if cut_record_copy != {}:
            new_record = record
            new_record['TITLE'] = cut_record_copy['TITLE']
            titled_records.append(new_record)
        else:
            titled_records.append(record)
    return titled_records

def ljust_record(record):
    new_record = copy.deepcopy(record)
    for field in record.keys():
        length = get_field_len(field)
        astring = record[field]
        if length:
            astring = astring.ljust(length)[:length]
            new_record[field] = astring
    return new_record

def ljust_records(records):
    new_records = []
    for record in records:
        new_records.append(ljust_record(record))
    return new_records

def records2csv(records):
    comma_seperated_strings = []
    for record in records:
        astring = ''
        for key in record.values():
            astring += key+','
        astring = sub("[,|\s]*$","",astring) #remove extra commas at end
        comma_seperated_strings.append(astring)
    return comma_seperated_strings

def strip_repeated_times(records):
    previous_time = ''
    stripped_records = []
    for rec in records:
        newrec = copy.deepcopy(rec)
        curr_time = newrec['TIME']
        if previous_time == curr_time:
            newrec['TIME'] = "        "
        if ":" in curr_time:
            previous_time = curr_time
        stripped_records.append(newrec)
    return stripped_records

def create_summary(records, strip_repeat_times=False,fields=[]):
    if verbose:
        print("creating summary")
    if fields == []:
        fields = ['TIME','CUT','COMMENT','FUNCTION','TITLE']
    if strip_repeat_times:
        records = strip_repeated_times(records)
    summary = []
    for rec in records:
        row = ''
        lrec = ljust_record(rec)
        for field in fields:
            try:
                next_str = lrec[field]
            except:
                next_str = ''
            row = row + next_str + ' '
        summary.append(row)
    return summary

def print_summary(records):
    try:
        for rec in records:
            print(rec)
        return True
    except:
        return False

def summarize_files(files,fields=[]):
    summaries = []
    for filename in files:
        if verbose:
            print("summarizing",filename)
        records = get_records_from_dbf(filename)
        summary = create_summary(records,True,fields)
        #print_summary(summary)
        nopath = sub(".*\/","",filename)
        noext = sub("\..*$","",nopath)
        mylog(summary,noext+"_summary.log")
        summaries.append(summary)
    return summaries

def find_koop_files(path):
    all_files = glob.glob(path+"/*KOOP.dbf")
    all_files += glob.glob(path+"/*KOOP.DBF")
    koop_files = []
    for file in all_files:
        koop_files.append(file)
    return koop_files

def find_template_files(path):
    all_files = glob.glob(path+"/*_template.txt")
    template_files = []
    for file in all_files:
        template_files.append(file)
    return template_files


def summarize_koop_files(path):
    files = find_koop_files(path)
    summarize_files(files)
    return None

def create_template(filename, cut_records, fields=[]):
    if fields == []:
        template_fields = ['TIME','CUT','COMMENT','FUNCTION']
    records = get_records_from_dbf(filename)
    template_records = []
    for record in records:
        template_record = {key:record[key] for key in template_fields}
        template_records.append(template_record)
    annotated_records = cross_ref_cut_titles(template_records,cut_records)
    return annotated_records

def create_templates(path, cut_records,strip_repeat_times=False):
    files = find_koop_files(path)
    for file in files:
        template = create_template(file, cut_records)
        if strip_repeated_times:
            stripped_template = strip_repeated_times(template)
        ljustified_template = ljust_records(stripped_template)
        template_csv = records2csv(ljustified_template)
        nopath = sub(".*\/","",file)
        noext = sub("\..*$","",nopath)
        template_name = sub("[\D]*","",noext) + "TEMPLATE.txt"
        mylog(template_csv,template_name)
    return None

def read_csvfile(filename):
    csv_as_lists = []
    with open(filename) as csvfile:
        csv_data = csv.reader(csvfile)
        for row in csv_data:
            csv_as_lists.append(row) 
    return csv_as_lists

def read_templates():
    template_files = find_template_files('./templates')
    templates = {}
    for filename in template_files:
        stripped = sub('^.*/','',filename)
        stripped = sub('^.*\\\\','',stripped)
        day_str = sub('_template.txt','',stripped).title()
        templates[day_str] = read_csvfile(filename)
        mylog(templates[day_str],day_str + '_template.log')
    return templates

def weekdays():
    return ['Monday', 'Tuesday', 'Wednesday', 'Thursday'
           ,'Friday', 'Saturday', 'Sunday'] 

def reorder_dict(adict, alist):
    ordered_dict = {}
    for astring in alist:
        try:
            ordered_dict[astring] = adict[astring]
        except:
            ordered_dict[astring] = ''
    return ordered_dict

def read_traffic(clip_top=False):
    traffic_data = []
    with open('inputfiles/traffic.csv') as csvfile:
        traffic_csv = csv.reader(csvfile)
        for row in traffic_csv:
            traffic_data.append(row)
    if clip_top:
        del traffic_data[:4]
        mylog(traffic_data,"traffic_data.log")
    return traffic_data

#real name would be convert_a_list_of_lists_to_a_list_of_dicts
#but that seemed a little much
def convert_lists_to_dicts(list_of_lists,fields=[]):
    #print("entering convert_lists_to_dicts")
    if fields == []:
        fields = get_internal_fields()
    #print("fields are ",fields)
    list_of_lists_of_dicts = []
    #pprint(fields)
    #print("listoflists type is:",type(list_of_lists))
    for alist in list_of_lists: 
        #print("alist is ",alist)
        list_of_lists_of_dicts.append(dict(zip(fields,alist)))
        #print( 'dict(zip(fields,alist)) is :',dict(zip(fields,alist)) )
    return list_of_lists_of_dicts 

def munge_traffic_title(title_string):
    rstring = title_string.lower().strip()
    #rstring = sub('prm roots train','roots train promo',rstring)
    rstring = sub('prm the gazing ball 5.22','prm gazing ball 5.22',rstring)
    rstring = sub('hammell on trial','hamell on trial',rstring)
    rstring = sub('roses and thorns','roses & thorns',rstring)
    rstring = sub('prm international folk','prm intl. folk',rstring)
    rstring = sub('/',' ',rstring)
    rstring = sub('a curious','curious',rstring)
    rstring = sub('ua clown dog.*','ua clown dog',rstring)
    rstring = sub('bookpeople','book people',rstring)
    rstring = sub(' +',' ',rstring)
    return rstring
 
#get rid of leading and trailing blanks all caps, other
def mylower(alist):
    remove_these = []
    for i,astring in enumerate(alist):
        astring = munge_traffic_title(astring)
        alist[i] = astring
        if astring.isspace() or not astring:
            remove_these.append(i)
    while remove_these:
        index = remove_these.pop()
        del alist[index]

#how to name these birds?  
#so we have a list of templates read in from the *template.txt files
#and now we have a list of templates wherein each template has been
#converted from list without field names into dictionaries with 
#the fields defined by a list
def convert_templates(templates):
    ordered_templates = reorder_dict(templates,weekdays())
    new_dicts = list(map(convert_lists_to_dicts,ordered_templates.values()))
    for template_day in new_dicts:
        for line in template_day:
            cut = dd(line,'CUT')
            if cut and not cut.isspace():
                line['TYPE'] = 'P'
            else:
                line['TYPE'] = 'C'
    return new_dicts
    #for template in templates:
    #    new_template = convert_lists_to_dicts(template, template_fields)
    #    new_templates.append(new_template)
    #return new_templates
    #return [convert_lists_to_dicts(template,template_fields) for template in templates]

def test_koop_db():
    print("Running quick koop_db test...\n\n\n\n")
    shutil.rmtree('logs')
    os.mkdir('logs')
    summarize_koop_files('./input_files')
    if exists('K:/DAD/Files/CUTS.DBF'):
        on_prod = True
        cut_records = get_records_from_dbf('K:/DAD/Files/cuts.dbf')
    else:
        on_prod = False
        cut_records = get_records_from_dbf('./input_files/cuts.dbf')
    create_templates('./input_files',cut_records,True)
    #note there was a hand modification done here for the first
    #creation.  from now on, modification is only done for show changes
    #we could also make a show calendar to template generation automation if
    #it would be helpful to the show schedule manager person(s)
    templates = read_templates()
    templates_with_records = convert_templates(templates)
    print("Running Checksum--The first line of numbers is the expected checksum of the output files.")
    print("                  The following line is what was observed.")
    print("\nIf the following two lines are the same the test passed--if not, it failed.\n")
    print("\n3503424626 13671")
    os.system('cat logs/0708* | cksum')
    print("\n\nFinished running koop_db test!\n\n")
   #records2ascii()
    return None

