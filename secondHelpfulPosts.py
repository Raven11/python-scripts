from datetime import datetime, timedelta
import MySQLdb
import statistics

cnx =  MySQLdb.connect(user='root', password='root123', 
                                host='127.0.0.1', database='Apple')

cur = cnx.cursor()

cur.execute("select ThreadID from posts1 where ThreadID is not null and ThreadID!=0 group by ThreadID having count(MessageID)>1")
thrds_list = cur.fetchall()
posts_threads = [ID for tupl in thrds_list for ID in tupl]

cur.execute("select ThreadID from posts1 where Apple_Employee = 1")
thrds_list = cur.fetchall()
employee_threads = [ID for tupl in thrds_list for ID in tupl]

def report_data(thread_id, emp, helpful, dates):
  filtered_dates = [dt for tupl in dates for dt in tupl][:5]
  time_diff = [int((j-i).total_seconds()/60) for i, j in zip(filtered_dates[:-1], filtered_dates[1:])]
  time_diff_length = len(time_diff)
  mean_time = format(statistics.mean(time_diff),'.3f')  if time_diff_length>=1 else 0
  median_time = format(statistics.median(time_diff),'.3f')  if time_diff_length>=1 else 0
  stdev_time = format(statistics.stdev(time_diff),'.3f')  if time_diff_length>1 else 0
  print(thread_id, emp, helpful, *time_diff, time_diff_length, mean_time, median_time, stdev_time, sep='||')

for elem in posts_threads:
    cur.execute("select helpful from posts1 where ThreadID={0} and helpful is not null order by MessageID, newdate limit 1,1".format(elem))
    record=cur.fetchone()
    if record is not None:
      helpful=record[0]
      if elem in employee_threads:
        cur.execute("select newdate from posts1 where ThreadID={0} and newdate is not null order by MessageID, newdate".format(elem))
        dates = cur.fetchall()
        report_data(elem, 1, helpful, dates)
      else:
        cur.execute("select newdate from posts1 where ThreadID={0} and Apple_Employee is not null and newdate is not null order by MessageID, newdate".format(elem))
        dates = cur.fetchall()
        report_data(elem, 0, helpful, dates)

