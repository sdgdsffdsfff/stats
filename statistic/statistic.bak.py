#!/usr/bin/env python
import os
import sys
import profile
import MySQLdb
import warnings 
import itertools
import collections
import multiprocessing
from StringIO import StringIO
from datetime import *

class mysqldb(object):
    def __init__(self,h,u,p,db,cs):
        self.conn = MySQLdb.connect(host=h,user=u,passwd=p,db=db,charset=cs)
        self.cursor = self.conn.cursor()
        #create all tables detail in tables.sqle
        warnings.filterwarnings('ignore')
        self.cursor.execute("call createTables;")

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()

def mapper(dbhandler):
    '''
    map output format (time,event,client_ip,rule_id)
    '''
    readline_num = dbhandler.cursor.execute("select time,TRIM(match_type),TRIM(attack_type),ip,rule_id from log_info where DATE_SUB(now(),INTERVAL 10 HOUR) < time")
    with open('./loading.data','w') as tmpIOBuffer:
        for timeinfo,match_type,attack_type,ip,rule_id in dbhandler.cursor.fetchall():
            event_type = ''
            if attack_type == '-':
                event_type = 'MODSEC_ALERT_EVENT_' + match_type.split('_')[-1]
            else:
                event_type = 'MODSEC_ALERT_EVENT_' + attack_type.split('_')[-1]

            print >>tmpIOBuffer,timeinfo,',',event_type,',',ip,',',rule_id
            #print timeinfo,',',event_type,',',ip,',',url,',',ruleid
        
    load_data_to_tmp_table_sql = r'LOAD DATA LOCAL INFILE "%s" INTO TABLE event_log_info FIELDS TERMINATED BY "," LINES TERMINATED BY "\n"(time,event,ip,ruleid)'%tmpIOBuffer.name
    #print load_data_to_tmp_table_sql
    dbhandler.cursor.execute(load_data_to_tmp_table_sql)
    os.remove(tmpIOBuffer.name)

def reducer(dbhandler):
    '''
    input data format is:
    (event,[[time,ip,url,rule_id],...])
    '''
    dbhandler.commit()
    dbhandler.close()

class MapReduce(object):
    def __init__(self,mapper,reducer):
        self.mapper = mapper
        self.reducer = reducer
        self.dbhandler = mysqldb('localhost','root','root','event_statistic','utf8')

    def partition(self):
        timeformat = "%Y-%m-%d %H:%i:%S"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        topN = 5
        span = 20 #unit second(0-59)

        proc = "call generateResultTables(DATE_FORMAT('%s','%s'),%d);"%(now,timeformat,span)
        print proc
        self.dbhandler.cursor.execute(proc)


    def __call__(self):
        self.mapper(self.dbhandler)
        self.partition()
        #result = self.reducer(self.dbhandler)

if __name__ == "__main__":
    run = MapReduce(mapper,reducer)
    run()
    #profile.run("run()")
