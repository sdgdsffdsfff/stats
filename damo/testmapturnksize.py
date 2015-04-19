#!/usr/bin/env python

from multiprocessing import *
import os 
import time

def test((name,seconds)):
	print os.getpid(),'\t',name,'\t',seconds

if __name__ == "__main__":
	pool = Pool(3)
	name = ["a","b","c","d","e","f","g","h","i","g","k","l","m","n"]
	pool.map(test,zip(name,range(10)),chunksize=3)
