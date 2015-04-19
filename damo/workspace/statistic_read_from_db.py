#!/usr/bin/env python
import sys
import os
import itertools
import collections
import MySQLdb
from StringIO import StringIO
import multiprocessing
from datetime import *
import tempfile
import profile

insert_event_times_sql_month = "insert into event_times_last_month(event,times) values(%s,%s)"
insert_event_times_sql_week = "insert into event_times_last_week(event,times) values(%s,%s)"
insert_event_times_sql_day = "insert into event_times_last_day(event,times) values(%s,%s)"
insert_event_times_sql_12hours = "insert into event_times_last_12hours(event,times) values(%s,%s)"
insert_event_times_sql_6hours = "insert into event_times_last_6hours(event,times) values(%s,%s)"

insert_event_ip_iptimes_sql_month = "insert into event_ip_iptimes_last_month(event,ip,iptimes) values(%s,%s,%s)"
insert_event_ip_iptimes_sql_week = "insert into event_ip_iptimes_last_week(event,ip,iptimes) values(%s,%s,%s)"
insert_event_ip_iptimes_sql_day = "insert into event_ip_iptimes_last_day(event,ip,iptimes) values(%s,%s,%s)"
insert_event_ip_iptimes_sql_12hours = "insert into event_ip_iptimes_last_12hours(event,ip,iptimes) values(%s,%s,%s)"
insert_event_ip_iptimes_sql_6hours = "insert into event_ip_iptimes_last_6hours(event,ip,iptimes) values(%s,%s,%s)"

insert_event_url_urltimes_sql_month = "insert into event_url_urltimes_last_month(event,url,urltimes) values(%s,%s,%s)"
insert_event_url_urltimes_sql_week = "insert into event_url_urltimes_last_week(event,url,urltimes) values(%s,%s,%s)"
insert_event_url_urltimes_sql_day = "insert into event_url_urltimes_last_day(event,url,urltimes) values(%s,%s,%s)"
insert_event_url_urltimes_sql_12hours = "insert into event_url_urltimes_last_12hours(event,url,urltimes) values(%s,%s,%s)"
insert_event_url_urltimes_sql_6hours = "insert into event_url_urltimes_last_6hours(event,url,urltimes) values(%s,%s,%s)"

insert_event_ruleid_ruleidtimes_sql_month = "insert into event_ruleid_ruleidtimes_last_month(event,ruleid,ruleidtimes) value(%s,%s,%s)"
insert_event_ruleid_ruleidtimes_sql_week = "insert into event_ruleid_ruleidtimes_last_week(event,ruleid,ruleidtimes) value(%s,%s,%s)"
insert_event_ruleid_ruleidtimes_sql_day = "insert into event_ruleid_ruleidtimes_last_day(event,ruleid,ruleidtimes) value(%s,%s,%s)"
insert_event_ruleid_ruleidtimes_sql_12hours = "insert into event_ruleid_ruleidtimes_last_12hours(event,ruleid,ruleidtimes) value(%s,%s,%s)"
insert_event_ruleid_ruleidtimes_sql_6hours = "insert into event_ruleid_ruleidtimes_last_6hours(event,ruleid,ruleidtimes) value(%s,%s,%s)"

class mysqldb(object):
    def __init__(self,h,u,p,db,cs):
        self.conn = MySQLdb.connect(host=h,user=u,passwd=p,db=db,charset=cs)
        self.cursor = self.conn.cursor()

        self.create_event_times_sql_month = "create table if not exists event_times_last_month(event char(64) primary key,times int(15))"
        self.create_event_times_sql_week = "create table if not exists event_times_last_week(event char(64) primary key,times int(15))"
        self.create_event_times_sql_day = "create table if not exists event_times_last_day(event char(64) primary key,times int(15))"
        self.create_event_times_sql_12hours = "create table if not exists event_times_last_12hours(event char(64) primary key,times int(15))"
        self.create_event_times_sql_6hours = "create table if not exists event_times_last_6hours(event char(64) primary key,times int(15))"

        self.create_event_ip_iptimes_sql_month = "create table if not exists event_ip_iptimes_last_month(event char(64),ip char(15),iptimes int(15))"
        self.create_event_ip_iptimes_sql_week = "create table if not exists event_ip_iptimes_last_week(event char(64),ip char(15),iptimes int(15))"
        self.create_event_ip_iptimes_sql_day = "create table if not exists event_ip_iptimes_last_day(event char(64),ip char(15),iptimes int(15))"
        self.create_event_ip_iptimes_sql_12hours = "create table if not exists event_ip_iptimes_last_12hours(event char(64),ip char(15),iptimes int(15))"
        self.create_event_ip_iptimes_sql_6hours = "create table if not exists event_ip_iptimes_last_6hours(event char(64),ip char(15),iptimes int(15))"

        self.create_event_url_urltimes_sql_month = "create table if not exists event_url_urltimes_last_month(event char(64),url varchar(4096),urltimes int(15))"
        self.create_event_url_urltimes_sql_week = "create table if not exists event_url_urltimes_last_week(event char(64),url varchar(4096),urltimes int(15))"
        self.create_event_url_urltimes_sql_day = "create table if not exists event_url_urltimes_last_day(event char(64),url varchar(4096),urltimes int(15))"
        self.create_event_url_urltimes_sql_12hours = "create table if not exists event_url_urltimes_last_12hours(event char(64),url varchar(4096),urltimes int(15))"
        self.create_event_url_urltimes_sql_6hours = "create table if not exists event_url_urltimes_last_6hours(event char(64),url varchar(4096),urltimes int(15))"

        self.create_event_ruleid_ruleidtimes_sql_month = "create table if not exists event_ruleid_ruleidtimes_last_month(event char(64),ruleid char(7),ruleidtimes int(15))"
        self.create_event_ruleid_ruleidtimes_sql_week = "create table if not exists event_ruleid_ruleidtimes_last_week(event char(64),ruleid char(7),ruleidtimes int(15))"
        self.create_event_ruleid_ruleidtimes_sql_day = "create table if not exists event_ruleid_ruleidtimes_last_day(event char(64),ruleid char(7),ruleidtimes int(15))"
        self.create_event_ruleid_ruleidtimes_sql_12hours = "create table if not exists event_ruleid_ruleidtimes_last_12hours(event char(64),ruleid char(15),ruleidtimes int(15))"
        self.create_event_ruleid_ruleidtimes_sql_6hours = "create table if not exists event_ruleid_ruleidtimes_last_6hours(event char(64),ruleid char(7),ruleidtimes int(15))"

        self.drop_table("event_times_last_month")
        self.drop_table("event_times_last_week")
        self.drop_table("event_times_last_day")
        self.drop_table("event_times_last_12hours")
        self.drop_table("event_times_last_6hours")

        self.drop_table("event_ip_iptimes_last_month")
        self.drop_table("event_ip_iptimes_last_week")
        self.drop_table("event_ip_iptimes_last_day")
        self.drop_table("event_ip_iptimes_last_12hours")
        self.drop_table("event_ip_iptimes_last_6hours")

        self.drop_table("event_url_urltimes_last_month")
        self.drop_table("event_url_urltimes_last_week")
        self.drop_table("event_url_urltimes_last_day")
        self.drop_table("event_url_urltimes_last_12hours")
        self.drop_table("event_url_urltimes_last_6hours")

        self.drop_table("event_ruleid_ruleidtimes_last_month")
        self.drop_table("event_ruleid_ruleidtimes_last_week")
        self.drop_table("event_ruleid_ruleidtimes_last_day")
        self.drop_table("event_ruleid_ruleidtimes_last_12hours")
        self.drop_table("event_ruleid_ruleidtimes_last_6hours")

        self.create_table(self.create_event_times_sql_month)
        self.create_table(self.create_event_times_sql_week)
        self.create_table(self.create_event_times_sql_day)
        self.create_table(self.create_event_times_sql_12hours)
        self.create_table(self.create_event_times_sql_6hours)

        self.create_table(self.create_event_ip_iptimes_sql_month)
        self.create_table(self.create_event_ip_iptimes_sql_week)
        self.create_table(self.create_event_ip_iptimes_sql_day)
        self.create_table(self.create_event_ip_iptimes_sql_12hours)
        self.create_table(self.create_event_ip_iptimes_sql_6hours)

        self.create_table(self.create_event_url_urltimes_sql_month)
        self.create_table(self.create_event_url_urltimes_sql_week)
        self.create_table(self.create_event_url_urltimes_sql_day)
        self.create_table(self.create_event_url_urltimes_sql_12hours)
        self.create_table(self.create_event_url_urltimes_sql_6hours)

        self.create_table(self.create_event_ruleid_ruleidtimes_sql_month)
        self.create_table(self.create_event_ruleid_ruleidtimes_sql_week)
        self.create_table(self.create_event_ruleid_ruleidtimes_sql_day)
        self.create_table(self.create_event_ruleid_ruleidtimes_sql_12hours)
        self.create_table(self.create_event_ruleid_ruleidtimes_sql_6hours)

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
    #UNIX_TIMESTAMP()
    #FROM_UNIXTIME(timestamp) 2003-09-09 11:20:22
    #DATE_SUB('2004-03-14 12:22:1', INTERVAL 6 HOUR)
    '''
    tmpIOBuffer = open('./lijian.debug','w')#tempfile.NamedTemporaryFile()
    #tmpIOBuffer = tempfile.NamedTemporaryFile()

    storeEachTimes = 0
    tmpBuffer = []
    readline_num = dbhandler.cursor.execute("select time,TRIM(match_type),TRIM(attack_type),ip,url,rule_id from log_info where DATE_SUB(now(),INTERVAL 1 DAY) < time")
    for timeinfo,match_type,attack_type,ip,url,ruleid in dbhandler.cursor.fetchall():
        storeEachTimes += 1
        event_type = ''
        if attack_type == '-':
            event_type = 'MODSEC_ALERT_EVENT_' + match_type.split('_')[-1]
        else:
            event_type = 'MODSEC_ALERT_EVENT_' + attack_type.split('_')[-1]

        tmpBuffer.append("%s,%s,%s,%s,%d\n"%(timeinfo,event_type,ip,url,ruleid))
        if storeEachTimes % 500 == 0:
            tmpIOBuffer.writelines(tmpBuffer)
            tmpBuffer = []
        
        #if heat the end of the table,write all the remaind
        #if reachtheendoffile:      #
            #tmpIOBuffer.writelines(tmpBuffer)
            #del(tmpBuffer)

        #print >>tmpIOBuffer,timeinfo,',',event_type,',',ip,',',url,',',ruleid
        #print timeinfo,',',event_type,',',ip,',',url,',',ruleid

    #load data to temporary table
    load_data_to_tmp_table_sql = r'LOAD DATA LOCAL INFILE "%s" INTO TABLE event_log_info FIELDS TERMINATED BY "," LINES TERMINATED BY "\n"'%tmpIOBuffer.name
    #this temporary table will store in mem and be delete untill close the session
    dbhandler.cursor.execute("create table if not exists event_log_info(rownu int not null auto_increment primary key,time DATETIME not null,event char(64) not null,ip char(20) not null, url varchar(200) not null, ruleid int(7))")
    dbhandler.cursor.execute(load_data_to_tmp_table_sql)
    tmpIOBuffer.close()


def reducer(dbhandler):
    '''
    input data format is:
    (event,[[time,ip,url,rule_id],...])
    '''
    dbhandler.cursor.execute("select * from event_log_info")
    i = 0 
    for time,event_type,ip,url,ruleid in dbhandler.cursor.fetchall():
        i += 1
        #print "reducer:\t",time,event_type,ip,url,ruleid
    print i

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
        self.dbhandler.cursor.execute("select *,count(*) from event_log_info GROUP BY event")
        for time,event,ip,url,ruleid,times in self.dbhandler.cursor.fetchall():
            print "partition:\t",time,event,ip,url,ruleid,times

    def __call__(self):
        self.mapper(self.dbhandler)
        #self.partition()
        #result = self.reducer(self.dbhandler)

if __name__ == "__main__":
    run = MapReduce(mapper,reducer)
    run()
    #profile.run("run()")
