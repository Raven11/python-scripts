from datetime import datetime
import mysql.connector
import csv

threadDict = {0:0}
with open('badThreads.csv') as csvfile:
     reader = csv.reader(csvfile)
     for row in reader:
             for thread in row:
                     threadDict[int(thread)] = int(thread)


cnx =  mysql.connector.connect(user='root', password='root123', 
                                host='127.0.0.1', database='Apple')

cur = cnx.cursor() 
cur.execute("select ThreadID, min(date) from posts1 group by ThreadId")
rows = cur.fetchall()

for row in rows:
    fmt1 = '%Y-%m-%d %H:%M:%S'
    fmt = '%b %d, %Y %I:%M %p'
    if (row[1] is not None and row[1] and not threadDict.has_key(row[0])):
        dateposted = datetime.strptime(row[1],fmt)
    
        cur.execute("select min(date) from posts1 where ThreadID={0} and helpful=1".format(row[0]))
        dateposted_helpful = cur.fetchone()[0]
        
        if dateposted_helpful is not None:
            cur.execute("select count(*) from posts1 where ThreadID={0} and date<='{1}' and date>='{2}'".format(row[0], dateposted_helpful, row[1]))
            count_helpful = cur.fetchone()[0]
        else:
            count_helpful = 0
    
        cur.execute("select min(date) from posts1 where ThreadID={0} and correct=1".format(row[0]))
        dateposted_correct = cur.fetchone()[0]
    
        if dateposted_correct is not None:
            cur.execute("select count(*) from posts1 where ThreadID={0} and date<='{1}' and date>='{2}'".format(row[0],dateposted_correct,row[1]))
            count_correct = cur.fetchone()[0]
        else:
            count_correct = 0
    

        cur.execute("select min(date) from posts1 where ThreadID={0} and (helpful=1 or correct=1)".format(row[0]))
        dateposted_correct_or_helpful = cur.fetchone()[0]
    
        if dateposted_correct_or_helpful is not None:
            cur.execute("select count(*) from posts1 where ThreadID={0} and date<='{1}' and date>='{2}'".format(row[0],dateposted_correct_or_helpful,row[1]))
            count_correct_or_helpful = cur.fetchone()[0]
        else:
            count_correct_or_helpful = 0
    

        minutesDiffHelpful = (datetime.strptime(dateposted_helpful, fmt) - dateposted).days * 24 * 60 if (dateposted_helpful is not None and row[1] is not None) else 'None'
        minutesDiffCorrect = (datetime.strptime(dateposted_correct, fmt) - dateposted).days * 24 * 60 if (dateposted_correct is not None and row[1] is not None) else 'None'
        minutesDiffHelpfulOrCorrect = (datetime.strptime(dateposted_correct_or_helpful, fmt) - dateposted).days * 24 * 60 if (dateposted_correct_or_helpful is not None and row[1] is not None) else 'None'
    
        print row[0], minutesDiffHelpful, count_helpful, minutesDiffCorrect,count_correct, minutesDiffHelpfulOrCorrect, count_correct_or_helpful 



cnx.close()
