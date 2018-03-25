from datetime import datetime, timedelta
import csv
import re
import sys
import enchant
from textstat.textstat import textstat
import MySQLdb


NewFileName1 = './SpellCheckError'+'.txt'
f1= open(NewFileName1,'w+',encoding="utf-8")
correctWordlist='./CorrectWordList'+'.txt'
f2= open(correctWordlist,'r',encoding = "ISO-8859-1")
cword=f2.readlines()
f2.close()
listcorrect=[]
for lines in cword:
    x=lines.split(",")
for words in x:
    listcorrect.append(words.strip().lower())

def calcSmogRadabilityIndex(mesage):
    smogMeasure=textstat.smog_index(str(message))
    return smogMeasure

def spellscheck(words):
    w=["coreAudio", "coreData", "dvd", "dylib", "google", "imac", "thanks"]
    error=0
    d = enchant.Dict("en_US")
    for word in words:
        word=word.decode()
        if d.check(word)==False:
            word=word.lower()
            if word.isalnum()==False:
                pass
            elif word.find("0")!=-1 or word.find("1")!=-1 or word.find("2")!=-1 or word.find("3")!=-1 or word.find("4")!=-1 or word.find("5")!=-1 or word.find("6")!=-1 or word.find("7")!=-1 or word.find("8")!=-1 or word.find("9")!=-1:
               pass
            elif word.find("gb")!=-1 and word[(word.find("gb"))-1].isdigit():
               pass
            elif word.find("ghz")!=-1 and word[(word.find("ghz"))-1].isdigit():
               pass
            elif word.find("apple")!=-1:
               pass
            elif word.find(w[0])!=-1 or word.find(w[1])!=-1 or word.find(w[2])!=-1 or word.find(w[3])!=-1 or word.find(w[4])!=-1 or word.find(w[5])!=-1 or word.find(w[6])!=-1:
               pass
            elif word in listcorrect:
                pass
            else:
               f1.write(word)
               f1.write("\n")
               error+=1
    return error


def countURLs(message):
    count1=message.count(".com")
    count2=message.count(".gov")
    count3=message.count(".ca")
    count4=message.count(".info")
    count5=message.count(".edu")
    count6=message.count(".org")
    count7=message.count(".net")
    count8=message.count(".biz")
    count9=message.count(".co")
    return(count1+count2+count3+count4+count5+count6+count7+count8+count9)

def countPolite(message):
    count=0
    words = ["thanks", "thank", "thx", "tks", "please", "pls"]
    wordList=[]
    wordList = message.strip().split()
    for word in wordList:
        word=word.lower()
        if word in words:
            count+=1
        elif word.find(words[0])!=-1:
            count+=1
        elif word.find(words[1])!=-1:
            count+=1
        elif word.find(words[2])!=-1:
            count+=1
        elif word.find(words[3])!=-1:
            count+=1
        elif word.find(words[4])!=-1:
            count+=1
        elif word.find(words[5])!=-1:
            count+=1
    return count

#sno ubstring, for short forms
def countWh(message):
    count=0
    words = ["what", "when", "where", "how", "wht", "whn","which","who","wch","whom","whose","why"]
    wordList=[]
    wordList = re.sub("[^\w]", " ",  message.strip()).split()
    for word in wordList:
        word=word.lower()
        if word in words:
            count+=1
    return count


def remove_duplicates(thrds_list):
    list_new_ths = [ID for tupl in thrds_list for ID in tupl]
    uniq_list_new_ths = tuple(dict.fromkeys(list_new_ths)) #flattening a nested structure of tuples.
    return uniq_list_new_ths

cnx =  MySQLdb.connect(user='root', password='root123', host='127.0.0.1', database='Apple')
cur = cnx.cursor()

cur.execute("select ThreadID from posts1 group by ThreadID having count(MessageID)=1 limit 160000")
ThreadIDs = cur.fetchall()
ThreadIDList = remove_duplicates(ThreadIDs)

cur.execute("select ThreadID, UserID, MessageID, newdate, content, Sub_Category from posts1 where ThreadID in {0}".format(ThreadIDList))
#userid,messageid,threadid,newdate,content
rows = cur.fetchall()
list_rows = [tupl for tupl in rows]

uniq_dict_rows = {}
for item in list_rows:
  if(item[0] not in uniq_dict_rows):
    uniq_dict_rows[item[0]] = item[1:6]

list_TM=[]
for key in uniq_dict_rows.keys():
  row = uniq_dict_rows[key]
  user_id = row[0]
  message_id=row[1]
  thread_id=key
  subcateg = row[4]
  if row[2] is not None and row[3] is not None:
          list_TM.append((user_id,thread_id,row[2]))
          exact_date = row[2]
          day_of_week = exact_date.isoweekday()
          time_of_day = format(float(exact_date.hour) + float(exact_date.minute)/float(60),'.2f')
          month_of_year = exact_date.month
          five_day_before_exact_date = exact_date + timedelta(days=-5)
          day_before_exact_date = exact_date + timedelta(days=-1)
          
          cur.execute("select count(ThreadID) from initial_posts where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}'".format(day_before_exact_date, exact_date, subcateg))
          no_of_threads_before = cur.fetchone()[0]
          cur.execute("select count(ThreadID) from initial_posts where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}'".format(five_day_before_exact_date, exact_date, subcateg))
          no_of_threads_five_day_before = cur.fetchone()[0]

          cur.execute("select count(MessageID) from posts1 where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}'".format(day_before_exact_date, exact_date, subcateg))

          no_of_messages_before = cur.fetchone()[0]          
          cur.execute("select count(MessageID) from posts1 where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}'".format(five_day_before_exact_date, exact_date, subcateg))
          no_of_messages_five_day_before = cur.fetchone()[0]
            
          cur.execute("select newdate from initial_posts where newdate>='{0}' order by newdate limit 1 offset 16".format(exact_date))
          skipdate = cur.fetchone()
          skipdate_timediff = int((skipdate[0] - exact_date).total_seconds()/60) if skipdate is not None else 'None'
            
          cur.execute("select count(MessageID) from posts1 where newdate>='{0}' and newdate<='{1}' and helpful=1 and Sub_Category='{2}'".format(day_before_exact_date, exact_date, subcateg))
          no_of_helpful_messages_before = cur.fetchone()[0]

          cur.execute("select count(MessageID) from posts1 where newdate>='{0}' and newdate<='{1}' and helpful=1 and Sub_Category='{2}'".format(five_day_before_exact_date, exact_date, subcateg))
          no_of_helpful_messages_five_day_before = cur.fetchone()[0]

          cur.execute("select count(MessageID) from posts1 where newdate>='{0}' and newdate<='{1}' and correct=1 and Sub_Category='{2}'".format(day_before_exact_date, exact_date, subcateg))
          no_of_correct_messages_before = cur.fetchone()[0]
          cur.execute("select count(MessageID) from posts1 where newdate>='{0}' and newdate<='{1}' and correct=1 and Sub_Category='{2}'".format(five_day_before_exact_date, exact_date, subcateg))
          no_of_correct_messages_five_day_before = cur.fetchone()[0]
            
          cur.execute("select count(MessageID) from posts1 where ThreadID='{0}'".format(thread_id))
          no_of_posts = cur.fetchone()[0] 

          cur.execute("select count(MessageID) from posts1 where UserID='{0}' and helpful=1 and newdate<='{1}'".format(user_id, exact_date))     
          CntHelp= cur.fetchone()[0]

          cur.execute("select count(MessageID) from posts1 where UserID='{0}' and correct=1 and newdate<='{1}'".format(user_id, exact_date))     
          CntCorrect= cur.fetchone()[0]

          cur.execute("select count(MessageID) from posts1 where UserID='{0}' and newdate<='{1}'".format(user_id, exact_date))     
          CntMessages= cur.fetchone()[0]

          cur.execute("select count(MessageID) from posts1 where ThreadID='{0}'".format(thread_id))
          CntMsgInThrd=cur.fetchone()[0]

          cur.execute("select min(newdate) from posts1 where ThreadID='{0}' and (helpful=1 or correct=1)".format(thread_id))
          useful_date=cur.fetchone()[0]
          TimeToUseful= int((useful_date - exact_date).total_seconds()/60) if useful_date is not None else 'None'
          
          cur.execute("select ThreadID from posts1 where UserID='{0}' and newdate<='{1}'".format(user_id, exact_date))
          new_ths = cur.fetchall()
          
          uniq_list_new_ths = remove_duplicates(new_ths)
          CntThrd = len(uniq_list_new_ths)
           
          if(CntThrd>1):
            cur.execute("select sum(helpful), sum(correct) from posts1 where ThreadID in {0}".format(uniq_list_new_ths))
            sums = cur.fetchone()
            
          else:
            cur.execute("select helpful,correct from posts1 where ThreadID={0}".format(uniq_list_new_ths[0]))
            sums = cur.fetchone()

          SumHelp = sums[0]
          SumCorrect = sums[1]
          
          message=row[3].encode(encoding='UTF-8') 
          if message!="":
              wordList=[]
              wordList = re.sub("[^\w]", " ",  message.decode().strip()).split()
              wordList1=[x.encode() for x in wordList]
              print(user_id, message_id, thread_id, subcateg, len(message.strip()),spellscheck(wordList1), countURLs(message.decode("utf-8")), countPolite(message.decode("utf-8")), countWh(message.decode("utf-8")), calcSmogRadabilityIndex(message.decode("utf-8")), time_of_day, day_of_week, month_of_year, no_of_posts, no_of_threads_before, no_of_threads_five_day_before, no_of_messages_before, no_of_messages_five_day_before,skipdate_timediff, no_of_helpful_messages_before, no_of_helpful_messages_five_day_before, no_of_correct_messages_before, no_of_correct_messages_five_day_before, exact_date,CntHelp, CntCorrect, CntMessages, CntMsgInThrd, TimeToUseful, CntThrd, SumHelp, SumCorrect, sep='||')
				 
				  
				
				
				
				
				
				
				
