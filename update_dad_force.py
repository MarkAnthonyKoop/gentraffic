from koop_db import read_traffic, weekdays
from verify_utils import getdates
from datetime import datetime
from os.path import exists
from sys import argv
import shutil

if __name__ == "__main__":

    startday = "MONDAY"

    if len(argv) > 1:
        startday = argv[1].upper()

    if "MON" not in startday.upper() and startday.upper() not in "MONDAY":
        print("\n\nWarning--date check overriddden!\n",
        "\t(date checking is bypassed whenever startday is not Monday)\n\n")

    print("\nStarting on",startday)


def update_dad(startday):
    on_prod = False
    if exists('K:/DAD/Files/CUTS.DBF'):
        on_prod = True

    ascii_traffic = read_traffic()
    weekof = ascii_traffic[1][2]

    (start_month,start_day,start_year) = weekof.split('/')

    weekof_dt = datetime(int(start_year), int(start_month), int(start_day))


    dates = getdates(ascii_traffic)
    print("\ndates are:",dates,'\n')
    week = weekdays()

    for i,day in enumerate(week):
        if day == 'Wednesday':
            week[i] = 'WEDNESDA'
        else:
            week[i] = day.upper()

    # get current datetime
    dt_now = datetime.now()
    print("\nToday's date is:", dt_now)

    # get weekday name
    print('\nToday is ', dt_now.strftime('%A'),dt_now.month,dt_now.day)
    print('Month/Day is ', str(dt_now.month).zfill(2)+str(dt_now.day).zfill(2),"\n")


    #if weekof_dt < dt_now and (startday.upper() in 'MONDAY' or 'MON' in startday.upper()):
    #    print('\n\nError! ==> Refusing to autocopy into the past.\n\n')
    #    return False
        
    started = False


    for date in dates:
        day = week.pop(0)
        if day in startday.upper() or startday.upper() in day:
            started = True
        if not started:
            continue
        auto_file = "outputfiles/"+date+"AUTO.DBF"
        day_file = "outputfiles/"+day+".DBF"
        shutil.copy(day_file,auto_file)
        if on_prod:
            shutil.copy(auto_file,"K:/DAD/Files/PLAYLIST")
        shutil.copy(auto_file, "logs")
        print("Updated",auto_file)
    print("\nFinished")

if __name__ == "__main__":
    update_dad(startday)
