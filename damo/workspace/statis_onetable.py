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
        #event ip url ruleid info table
        warnings.filterwarnings('ignore')
        self.cursor.execute("create table if not exists event_log_info(num int not null auto_increment primary key,time DATETIME not null,event char(64) not null,ip char(20)not null, url varchar(200) not null, ruleid int(7) not null)")
        #vent times
        warnings.filterwarnings('ignore')
        self.cursor.execute("create table if not exists event_times select time,event,count(event) as eventtimes from event_log_info limit 0")

        #event ip iptimes
        self.cursor.execute("create table if not exists event_ip_iptimes select time,event,ip,count(ip) as iptimes from event_log_info limit 0")

        #event rule ruleid 
        self.cursor.execute("create table if not exists event_ruleid_ruleidtimes select time,event,ruleid,count(ruleid) as ruleidtimes from event_log_info limit 0")

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
    with open('/home/longlijian/statistic/workspace/loading.data','w') as tmpIOBuffer:
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
        timeformat = "%Y-%m-%d %H:%i:%S"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #event times
        delete_event_times_sql = "delete from event_times where time between DATE_SUB(date_format('%s','%s'),INTERVAL '31 1:0:0' DAY_SECOND) and DATE_SUB(date_format('%s','%s'),INTERVAL 31 DAY);"%(now,timeformat,now,timeformat)
        self.dbhandler.cursor.execute(delete_event_times_sql)
        insert_event_times_sql = "insert into event_times select * from (select date_format('%s','%s')) times,(select event,count(event) as eventtimes from event_log_info where time between  DATE_SUB(date_format('%s','%s'),INTERVAL 10 SECOND) AND date_format('%s','%s') group by event order by count(event) DESC LIMIT 5) event_eventtimes"%(now,timeformat,now,timeformat,now,timeformat)
        self.dbhandler.cursor.execute(insert_event_times_sql)

        #event ip iptimes 
        delete_event_ip_iptimes_sql = "delete from event_ip_iptimes where time between DATE_SUB(date_format('%s','%s'),INTERVAL '31 0:0:10' DAY_SECOND) and DATE_SUB(date_format('%s','%s'),INTERVAL 31 DAY);"%(now,timeformat,now,timeformat)
        self.dbhandler.cursor.execute(delete_event_ip_iptimes_sql)
        #event_ip_iptimes view
        view_event_ip_iptimes_sql = "create or replace view view_for_fetch_data as select a.event,b.ip,count(b.ip) as iptimes from event_times a left join event_log_info b on a.event=b.event where b.time between DATE_SUB(date_format('%s','%s'),INTERVAL 10 SECOND) and date_format('%s','%s') group by a.event,b.ip order by a.event,iptimes desc;"%(now,timeformat,now,timeformat)
        self.dbhandler.cursor.execute(view_event_ip_iptimes_sql)

        insert_event_ip_iptimes_sql = "insert into event_ip_iptimes select * from (select date_format('%s','%s')) time,(select * from view_for_fetch_data v where 2 > (select count(*) from view_for_fetch_data where event=v.event and iptimes > v.iptimes) order by v.event,v.iptimes desc) event_ip_iptimes;"%(now,timeformat)
        self.dbhandler.cursor.execute(insert_event_ip_iptimes_sql)

        #event ruleid ruleidtimes
        delete_event_ruleid_ruleidtimes_sql = "delete from event_ruleid_ruleidtimes where time between DATE_SUB(date_format('%s','%s'),INTERVAL '31 0:0:10' DAY_SECOND) and DATE_SUB(date_format('%s','%s'),INTERVAL 31 DAY);"%(now,timeformat,now,timeformat)
        self.dbhandler.cursor.execute(delete_event_ruleid_ruleidtimes_sql)
        insert_event_ruleid_ruleidtimes_sql = "insert into event_ruleid_ruleidtimes select * from (select date_format('%s','%s')) times,(select event,ruleid,count(ruleid) as ruleidtimes from event_log_info where time between  DATE_SUB(date_format('%s','%s'),INTERVAL 10 SECOND) AND date_format('%s','%s') group by event,ruleid order by count(event) DESC LIMIT 5) event_eventtimes;"%(now,timeformat,now,timeformat,now,timeformat)
        self.dbhandler.cursor.execute(insert_event_ruleid_ruleidtimes_sql)

    def __call__(self):
        self.mapper(self.dbhandler)
        self.partition()
        result = self.reducer(self.dbhandler)

if __name__ == "__main__":
    run = MapReduce(mapper,reducer)
    run()
    #profile.run("run()")
