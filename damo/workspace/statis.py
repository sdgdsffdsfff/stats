#!/usr/bin/env python
import sys
import os
import itertools
import collections
import MySQLdb
from StringIO import StringIO
import multiprocessing
from datetime import *
import warnings 
import profile

class mysqldb(object):
    def __init__(self,h,u,p,db,cs):
        self.conn = MySQLdb.connect(host=h,user=u,passwd=p,db=db,charset=cs)
        self.cursor = self.conn.cursor()

        #event ip iptimes
        warnings.filterwarnings('ignore')
        self.cursor.execute("create table if not exists event_ip_iptimes_last_6hours select event,ip,count(ip) as iptimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_ip_iptimes_last_12hours select event,ip,count(ip) as iptimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_ip_iptimes_last_1day select event,ip,count(ip) as iptimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_ip_iptimes_last_1week select event,ip,count(ip) as iptimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_ip_iptimes_last_1month select event,ip,count(ip) as iptimes from event_log_info limit 0")

        #event url urltimes
        self.cursor.execute("create table if not exists event_url_urltimes_last_6hours select event,url,count(url) as urltimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_url_urltimes_last_12hours select event,url,count(url) as urltimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_url_urltimes_last_1day select event,url,count(url) as urltimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_url_urltimes_last_1week select event,url,count(url) as urltimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_url_urltimes_last_1month select event,url,count(url) as urltimes from event_log_info limit 0")

        #event rule ruleid 
        self.cursor.execute("create table if not exists event_ruleid_ruleidtimes_last_6hours select event,ruleid,count(ruleid) as ruleidtimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_ruleid_ruleidtimes_last_12hours select event,ruleid,count(ruleid) as ruleidtimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_ruleid_ruleidtimes_last_1day select event,ruleid,count(ruleid) as ruleidtimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_ruleid_ruleidtimes_last_1week select event,ruleid,count(ruleid) as ruleidtimes from event_log_info limit 0")
        self.cursor.execute("create table if not exists event_ruleid_ruleidtimes_last_1month select event,ruleid,count(ruleid) as ruleidtimes from event_log_info limit 0")

    def drop_table(self,table_name):
        if table_name:
            self.cursor.execute("drop table if exists %s"%table_name)

    def create_table(self,create_table_sql):
        if create_table_sql:
            self.cursor.execute(create_table_sql)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()

def mapper(dbhandler):
    '''
    map output format (time,event,client_ip,url,rule_id)
    '''
    readline_num = dbhandler.cursor.execute("select time,TRIM(match_type),TRIM(attack_type),ip,url,rule_id from log_info where DATE_SUB(now(),INTERVAL 10 SECOND) < time")
    with open('./loading.data','w') as tmpIOBuffer:
        for timeinfo,match_type,attack_type,ip,url,ruleid in dbhandler.cursor.fetchall():
            event_type = ''
            if attack_type == '-':
                event_type = 'MODSEC_ALERT_EVENT_' + match_type.split('_')[-1]
            else:
                event_type = 'MODSEC_ALERT_EVENT_' + attack_type.split('_')[-1]

            print >>tmpIOBuffer,timeinfo,',',event_type,',',ip,',',url,',',ruleid
            #print timeinfo,',',event_type,',',ip,',',url,',',ruleid

    load_data_to_tmp_table_sql = r'LOAD DATA LOCAL INFILE "%s" INTO TABLE event_log_info FIELDS TERMINATED BY "," LINES TERMINATED BY "\n"(time,event,ip,url,ruleid)'%tmpIOBuffer.name
    #print load_data_to_tmp_table_sql
    warnings.filterwarnings('ignore')
    dbhandler.cursor.execute("create table if not exists event_log_info(num int not null auto_increment primary key,time DATETIME not null,event char(64) not null,ip char(20)not null, url varchar(200) not null, ruleid int(7) not null)")
    dbhandler.cursor.execute(load_data_to_tmp_table_sql)

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
        '''
        temporary table definition: time,event_type,ip,url,ruleid
        '''
        '''
		#get 10s data all event use this 10s infomation
        self.dbhandler.cursor.execute("select time,TRIM(event),ip,url,ruleid from event_log_info where time BETWEEN DATE_SUB(now(),INTERVAL 10 SECOND) AND NOW()")

		#endof last 6hour
        self.dbhandler.cursor.execute("select time,TRIM(event),ip,url,ruleid from event_log_info where time BETWEEN DATE_SUB(now(),INTERVAL '1:0:10' HOUR_SECOND) AND DATE_SUB(now(),INTERVAL 1 HOUR)")

		#endof last 12hours
        self.dbhandler.cursor.execute("select time,TRIM(event),ip,url,ruleid from event_log_info where time BETWEEN DATE_SUB(now(),INTERVAL '12:0:10' HOUR_SECOND) AND DATE_SUB(now(),INTERVAL 12 HOUR)")
		#endof last day 
        self.dbhandler.cursor.execute("select time,TRIM(event),ip,url,ruleid from event_log_info where time BETWEEN DATE_SUB(now(),INTERVAL '1 0:0:10' DAY_SECOND) AND DATE_SUB(now(),INTERVAL 1 DAY)")
		#endof last week
        self.dbhandler.cursor.execute("select time,TRIM(event),ip,url,ruleid from event_log_info where time BETWEEN DATE_SUB(now(),INTERVAL '7 0:0:10' DAY_SECOND) AND DATE_SUB(now(),INTERVAL 7 DAY)")
		#endof last month
        self.dbhandler.cursor.execute("select time,TRIM(event),ip,url,ruleid from event_log_info where time BETWEEN DATE_SUB(now(),INTERVAL '30 0:0:10' DAY_SECOND) AND DATE_SUB(now(),INTERVAL 30 DAY)")
        spaninfo = []
        '''
        #event times
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_times_last_6hours;")
        self.dbhandler.cursor.execute("insert into event_times_last_6hours select event,count(event) as eventtimes from event_log_info where time between  DATE_SUB(now(),INTERVAL 6 HOUR) AND now() group by event order by count(event) DESC LIMIT 3")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_times_last_12hours;")
        self.dbhandler.cursor.execute("insert into event_times_last_12hours select event,count(event) as eventtimes from event_log_info where time between  DATE_SUB(now(),INTERVAL 12 HOUR) AND now() group by event order by count(event) DESC LIMIT 3")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_times_last_1day;")
        self.dbhandler.cursor.execute("insert into event_times_last_1day select event,count(event) as eventtimes from event_log_info where time between  DATE_SUB(now(),INTERVAL 1 DAY) AND now() group by event order by count(event) DESC LIMIT 3")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_times_last_1week;")
        self.dbhandler.cursor.execute("insert into event_times_last_1week select event,count(event) as eventtimes from event_log_info where time between  DATE_SUB(now(),INTERVAL 1 WEEK) AND now() group by event order by count(event) DESC LIMIT 3")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_times_last_1month;")
        self.dbhandler.cursor.execute("insert into event_times_last_1month select event,count(event) as eventtimes from event_log_info where time between  DATE_SUB(now(),INTERVAL 1 MONTH) AND now() group by event order by count(event) DESC LIMIT 3")
        #event ip iptimes 
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_ip_iptimes_last_6hours;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_ip_iptimes_last_12hours;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_ip_iptimes_last_1day;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_ip_iptimes_last_1week;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_ip_iptimes_last_1month;")
        #event rul urltimes
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_url_urltimes_last_6hours;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_url_urltimes_last_12hours;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_url_urltimes_last_1day;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_url_urltimes_last_1week;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_url_urltimes_last_1month;")
        #event ruleid ruleidtimes
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_ruleid_ruleidtimes_last_6hours;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_ruleid_ruleidtimes_last_12hours;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_ruleid_ruleidtimes_last_1day;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_ruleid_ruleidtimes_last_1week;")
        self.dbhandler.cursor.execute("TRUNCATE TABLE event_ruleid_ruleidtimes_last_1month;")

    def __call__(self):
        self.mapper(self.dbhandler)
        self.partition()
        #result = self.reducer(self.dbhandler)

if __name__ == "__main__":
    run = MapReduce(mapper,reducer)
    run()
    #profile.run("run()")
