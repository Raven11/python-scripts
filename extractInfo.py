from datetime import datetime
lines = []
with open('session_bvid_data_Home_Mobile_output.txt') as f:
    for line in f.readlines():
        nline = line.split("||")
        t1 = (datetime.strptime(nline[7],"%Y-%m-%d %H:%M:%S") - datetime.strptime(nline[6],"%Y-%m-%d %H:%M:%S")).total_seconds()
        t2 = (datetime.strptime(nline[8],"%Y-%m-%d %H:%M:%S") - datetime.strptime(nline[7],"%Y-%m-%d %H:%M:%S")).total_seconds()
        lines.append("||".join([nline[1],str(t1),str(t2)]))

with open('new_output.txt', 'w') as f:
    for line in lines:
        f.write(line+'\r\n')





    

