from datetime import datetime, timedelta
import csv
import sys
import enchant
import MySQLdb

def remove_duplicates(lst):
    lst_new = [ID for tupl in lst for ID in tupl if ID is not None]
    uniq_lst_new = tuple(dict.fromkeys(lst_new)) #flattening a nested structure of tuples.
    return uniq_lst_new

cnx =  MySQLdb.connect(user='root', password='root123', host='127.0.0.1', database='Apple')
cur = cnx.cursor()


cur.execute("select Sub_Category from posts1")
subcategs = cur.fetchall()
uniq_categs = remove_duplicates(subcategs)

for subcat in uniq_categs:
  cur.execute("select ThreadID, newdate from posts1 where Sub_Category='{0}' and newdate is not null order by newdate".format(subcat))
  thrds = cur.fetchall()
  thrd_ids = [tupl[0] for tupl in thrds if tupl[0] is not None]
  uniq_thrds = tuple(dict.fromkeys(thrd_ids))
  for thrd in uniq_thrds:
   sequence,value_array = 0,[]
   post_date,last_date = None,None
   for idx,val in enumerate(thrds):
    if thrd == val[0]:
        post_date,value_array = val[1],[]
        thrd_pos = idx
    elif post_date is not None:
        if not value_array or val[0] not in list(zip(*value_array))[0]:
          value_array.append(val)
          sequence+=1
        if sequence == 16:
          break
   try:
    last_date = value_array[sequence-1][1]
   except IndexError:
    last_date = None
   time_on_front = int((last_date - post_date).total_seconds()/60) if last_date is not None and post_date is not None else None
   print(thrd,time_on_front,sep='||')
        
  
