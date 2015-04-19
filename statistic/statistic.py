#!/usr/bin/env python
import os
import sys
import profile
import MySQLdb
import warnings 
import itertools
import collections
from datetime import *

class Statistic(object):
    def __init__(self,h,u,p,db,cs):
        self.conn = MySQLdb.connect(host=h,user=u,passwd=p,db=db,charset=cs)
        self.cursor = self.conn.cursor()
        #create all tables detail in tables.sqle
        warnings.filterwarnings('ignore')
        self.cursor.execute("call createTables;")

    def __call__(self):
        timeformat = "%Y-%m-%d %H:%i:%S"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time_span = 10

        proc = "call generateResultTables(DATE_FORMAT('%s','%s'),%d);"%(now,timeformat,time_span)
        print proc
        self.cursor.execute(proc)
        


if __name__ == "__main__":
    run = Statistic('localhost','root','root','event_statistic','utf8')
    run()
    #profile.run("run()")
