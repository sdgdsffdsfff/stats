#!/usr/bin/env python
import sys
import itertools
import collections
import MySQLdb
from StringIO import StringIO
import multiprocessing
from datetime import *
import warnings
import profile

insert_log_info_sql = "insert into log_info (time,match_type,attack_type,ip,url,rule_id) value(%s,%s,%s,%s,%s,%s)"
class mysqldb(object):
    def __init__(self,h,u,p,db,cs):
        self.conn = MySQLdb.connect(host=h,user=u,passwd=p,db=db,charset=cs)
        self.cursor = self.conn.cursor()
        self.create_log_info_sql = "create table if not exists log_info(time datetime,match_type char(20),attack_type char(20),ip char(20),url varchar(200),rule_id int(10),INDEX(time))"
        warnings.filterwarnings('ignore')
        self.cursor.execute(self.create_log_info_sql)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    from random import *
    db = mysqldb('localhost','root','root','event_statistic','utf8')
    match_type = ["MATCH_WEBTR","MATHC_RULE","MATCH_ACL","MATCH_F","MATCH_G"]
    attack_type = ["ATTACK_LEAKAGE","ATTACK_A","ATTACK_B","ATTACK_C","ATTACK_D","ATTACK_E","-"]
    url = ["/","/index","/capture.html","/first.html","/predirect.html","/tredirect.html","/deny.html","/inetput.html"]
    rang = 30000

    with open('/home/longlijian/statistic/workspace/log_info.data','w') as log_info:
        for i in xrange(rang):
            print >>log_info,\
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),',',\
                    match_type[randint(0,len(match_type)-1)],',',\
                    attack_type[randint(0,len(attack_type)-1)],',',\
                    '.'.join(list([str(randint(251,254)),str(randint(253,255)),str(randint(254,255)),str(randint(1,255))])),',',\
                    url[randint(0,len(url)-1)],',',\
                    randint(1000001,9999999)

    db.cursor.execute('LOAD DATA LOCAL INFILE "/home/longlijian/statistic/workspace/log_info.data" INTO TABLE log_info FIELDS TERMINATED BY "," LINES TERMINATED BY "\n"')
