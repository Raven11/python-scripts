from datetime import datetime, timedelta
import mysql.connector



cnx =  mysql.connector.connect(user='root', password='root123', 
                                host='127.0.0.1', database='Apple')

cur = cnx.cursor()

subcategories = ['iTunes Store','iMovie for Mac']

for subcat in subcategories:
    print subcat 
    cur.execute("select ThreadID, newdate from initial_posts where Sub_Category='{0}'".format(subcat))
    rows = cur.fetchall()	
	
    for row in rows:
	    if row[1] is not None:
	        exact_date = row[1]
	        day_of_week = exact_date.isoweekday()
	        time_of_day = format(float(exact_date.hour) + float(exact_date.minute)/float(60),'.2f')
	        month_of_year = exact_date.month
	        day_after_exact_date = exact_date + timedelta(days=1)
	        day_before_exact_date = exact_date + timedelta(days=-1)
            
            cur.execute("select count(*) from initial_posts where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}'".format(day_before_exact_date, exact_date, subcat))
            no_of_threads_before = cur.fetchone()[0]
            cur.execute("select count(*) from initial_posts where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}'".format(exact_date, day_after_exact_date, subcat))
            no_of_threads_after = cur.fetchone()[0]

            cur.execute("select count(*) from posts1 where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}'".format(day_before_exact_date, exact_date, subcat))
            no_of_messages_before = cur.fetchone()[0]          
            cur.execute("select count(*) from posts1 where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}'".format(exact_date, day_after_exact_date, subcat))
            no_of_messages_after = cur.fetchone()[0]
            
            cur.execute("select newdate from initial_posts where Sub_Category='{0}' and newdate>='{1}' order by newdate limit 1 offset 16".format(subcat, exact_date))
            skipdate = cur.fetchone()[0]
            skipdate_timediff = int((skipdate - exact_date).total_seconds()/60)
            
            cur.execute("select count(*) from posts1 where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}' and helpful=1".format(day_before_exact_date, exact_date, subcat))
            no_of_helpful_messages_before = cur.fetchone()[0]
            cur.execute("select count(*) from posts1 where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}' and helpful=1".format(exact_date, day_after_exact_date, subcat))
            no_of_helpful_messages_after = cur.fetchone()[0]

            cur.execute("select count(*) from posts1 where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}' and correct=1".format(day_before_exact_date, exact_date, subcat))
            no_of_correct_messages_before = cur.fetchone()[0]
            cur.execute("select count(*) from posts1 where newdate>='{0}' and newdate<='{1}' and Sub_Category='{2}' and correct=1".format(exact_date, day_after_exact_date, subcat))
            no_of_correct_messages_after = cur.fetchone()[0]
            
            cur.execute("select count(MessageID) from posts1 where ThreadID='{0}' and Sub_Category='{1}'".format(row[0], subcat))
            no_of_posts = cur.fetchone()[0]
            
            print row[0], exact_date, time_of_day, day_of_week, month_of_year, no_of_posts, no_of_threads_before, no_of_threads_after, no_of_messages_before, no_of_messages_after,skipdate_timediff, no_of_helpful_messages_before, no_of_helpful_messages_after, no_of_correct_messages_before, no_of_correct_messages_after


