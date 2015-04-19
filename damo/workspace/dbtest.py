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

if __name__ == "__main__":
    db = mysqldb('localhost','root','root','event_statistic','utf8')
    db.cursor.execute("set @num:=0,@type:='';select event,ip,count(ip) as iptimes, @num := if(@type = event,@num+1,@num) as counttimes,@type :=event as dummy from tst force index(event,ip) group by event,ip ")
    for x in db.cursor.fetchall():
        print "test:\t",
        print x
    print "end"
