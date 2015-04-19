#!/usr/bin/env python
import sys
import itertools
import collections
import MySQLdb
from StringIO import StringIO
import multiprocessing
from datetime import *
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

        self.create_event_times_sql_month = "create table if not exists event_times_last_month(event varchar(64) primary key,times int(100))"
        self.create_event_times_sql_week = "create table if not exists event_times_last_week(event varchar(64) primary key,times int(100))"
        self.create_event_times_sql_day = "create table if not exists event_times_last_day(event varchar(64) primary key,times int(100))"
        self.create_event_times_sql_12hours = "create table if not exists event_times_last_12hours(event varchar(64) primary key,times int(100))"
        self.create_event_times_sql_6hours = "create table if not exists event_times_last_6hours(event varchar(64) primary key,times int(100))"

        self.create_event_ip_iptimes_sql_month = "create table if not exists event_ip_iptimes_last_month(event varchar(64),ip varchar(20),iptimes int(100))"
        self.create_event_ip_iptimes_sql_week = "create table if not exists event_ip_iptimes_last_week(event varchar(64),ip varchar(20),iptimes int(100))"
        self.create_event_ip_iptimes_sql_day = "create table if not exists event_ip_iptimes_last_day(event varchar(64),ip varchar(20),iptimes int(100))"
        self.create_event_ip_iptimes_sql_12hours = "create table if not exists event_ip_iptimes_last_12hours(event varchar(64),ip varchar(20),iptimes int(100))"
        self.create_event_ip_iptimes_sql_6hours = "create table if not exists event_ip_iptimes_last_6hours(event varchar(64),ip varchar(20),iptimes int(100))"

        self.create_event_url_urltimes_sql_month = "create table if not exists event_url_urltimes_last_month(event varchar(64),url varchar(4096),urltimes int(100))"
        self.create_event_url_urltimes_sql_week = "create table if not exists event_url_urltimes_last_week(event varchar(64),url varchar(4096),urltimes int(100))"
        self.create_event_url_urltimes_sql_day = "create table if not exists event_url_urltimes_last_day(event varchar(64),url varchar(4096),urltimes int(100))"
        self.create_event_url_urltimes_sql_12hours = "create table if not exists event_url_urltimes_last_12hours(event varchar(64),url varchar(4096),urltimes int(100))"
        self.create_event_url_urltimes_sql_6hours = "create table if not exists event_url_urltimes_last_6hours(event varchar(64),url varchar(4096),urltimes int(100))"

        self.create_event_ruleid_ruleidtimes_sql_month = "create table if not exists event_ruleid_ruleidtimes_last_month(event varchar(64),ruleid varchar(20),ruleidtimes int(100))"
        self.create_event_ruleid_ruleidtimes_sql_week = "create table if not exists event_ruleid_ruleidtimes_last_week(event varchar(64),ruleid varchar(20),ruleidtimes int(100))"
        self.create_event_ruleid_ruleidtimes_sql_day = "create table if not exists event_ruleid_ruleidtimes_last_day(event varchar(64),ruleid varchar(20),ruleidtimes int(100))"
        self.create_event_ruleid_ruleidtimes_sql_12hours = "create table if not exists event_ruleid_ruleidtimes_last_12hours(event varchar(64),ruleid varchar(20),ruleidtimes int(100))"
        self.create_event_ruleid_ruleidtimes_sql_6hours = "create table if not exists event_ruleid_ruleidtimes_last_6hours(event varchar(64),ruleid varchar(20),ruleidtimes int(100))"

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

    def insert_data_to_table(self,insert_sql,pargm):
        if insert_sql:
            self.cursor.execute(insert_sql,pargm)

    def updata_table_data(self,updata_sql):
        if updata_sql:
            self.cursor.execute(updata_sql)

    def lookup(self,sql):
        if sql:
            ret = self.cursor.execute(sql)
            for row in self.cursor.fetchall():
                print row

    def delete_table_data(self,delete_sql):
        if delete_sql:
            self.cursor.execute(delete_sql)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()


IOBuffer = StringIO()
def mapper(rawinputdata,dbhandler):
    '''
    map output format (event,(time,client_ip,url,rule_ip,1)
    '''
    now = datetime.now()
    delta = timedelta(weeks=5)
    firsttime = (now - delta).strftime("%Y-%m-%d %H:%M:%S")

    result = []
    event_list = []
    for oneline in rawinputdata:
        tmplist = []
        list_line = oneline.strip().split(' ')
        #print list_line
        for element in list_line:
            if '=' in element:
                tmplist.append(element)
            else:
                tmplist[-1] = tmplist[-1] + ' ' + element

        time = client_ip = url = match_type = attack_type = rule_id = ''
        for e in tmplist:
            key_value = e.split('=')
            if key_value[0].strip() == 'time':
                time = key_value[1][1:-1]
            if key_value[0].strip() == 'client_ip':
                client_ip = key_value[1]
            if key_value[0].strip() == 'url':
                url = key_value[1][1:-1]
            if key_value[0].strip() == 'match_type':
                match_type = key_value[1][1:-1]
            if key_value[0].strip() == 'attack_type':
                attack_type = key_value[1][1:-1]
            if key_value[0].strip() == 'rule_id':
                rule_id = key_value[1][1:-1]
        #before the earnest time need to filte
        if time < firsttime:
            continue

        # "#MATCH_ACL (MODSEC_MATCH_TYPE_ACL  ===>> MODSEC_ALERT_EVENT_ACL ) if attack is empty"
        # "#otherwise ATTACK_LEAKAGE (MODSEC_ATTACK_XXX ===>> MODSEC_ALERT_EVENT_XXX)"
        #if attack type is - then the event can reduce by match type
        event_type = ''
        if attack_type == '-':
            event_type = 'MODSEC_ALERT_EVENT_' + match_type.split('_')[-1]
        #if attack type is not - then event type can reduce by attack type
        else:
            event_type = 'MODSEC_ALERT_EVENT_' + attack_type.split('_')[-1]
        #map output format (event,time,client_ip,url,rule_id)
        print >>IOBuffer,'\t'.join(list([event_type,time,client_ip,url,rule_id]))

        
def reducer(output_by_partition,dbhandler):
    '''
    input data format is:
    (event,[[time,ip,url,rule_id],...])
    '''
    #dbhandler = mysqldb('localhost','root','root','topwaf_config_db','utf8')
    for event,eventinfo in output_by_partition.items():
        dbhandler.insert_data_to_table(insert_event_times_sql_month,(event,len(eventinfo))) 

        ips = collections.defaultdict(int)
        urls = collections.defaultdict(int)
        rule_ids = collections.defaultdict(int)

        for element in eventinfo:
            ips[element[1]] += 1
            urls[element[2]] += 1
            rule_ids[element[3]] += 1
        
        for key,value in ips.items():
            dbhandler.insert_data_to_table(insert_event_ip_iptimes_sql_month,(event,key,value))

        for key,value in urls.items():
            dbhandler.insert_data_to_table(insert_event_url_urltimes_sql_month,(event,key,value)) 

        for key,value in rule_ids.items():
            dbhandler.insert_data_to_table(insert_event_ruleid_ruleidtimes_sql_month,(event,key,value))

    dbhandler.commit()
    dbhandler.close()

class MapReduce(object):
    def __init__(self,mapper,reducer):
        self.mapper = mapper
        self.reducer = reducer
        self.dbhandler = mysqldb('localhost','root','root','topwaf_config_db','utf8')

    def partition(self):#,output_by_mapper):
        '''
        the out put of partition is 
        (event,[[time,ip,url,rule_id],...])
        '''
        partition_data = collections.defaultdict(list)
        output_by_mapper = IOBuffer.getvalue().strip().split('\n')
        for oneline in output_by_mapper:
            tmp = oneline.split('\t')
            partition_data[tmp[0]].append(tmp[1:])
        return partition_data

    def __call__(self,inputs):
        output_by_mapper = self.mapper(inputs,self.dbhandler)
        partitioned_data = self.partition()
        result = self.reducer(partitioned_data,self.dbhandler)
        return result

if __name__ == "__main__":
    run = MapReduce(mapper,reducer)
    run(open('./9wlines'))
    #profile.run("run(open('./1wlines'))")
