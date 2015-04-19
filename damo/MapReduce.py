#!/usr/bin/env python
import collections
import itertools
import multiprocessing
from datetime import *

class MapReduce(object):
	def __init__(self,mapper,reducer,nworks=2):
		self.mapper = mapper
		self.reducer = reducer
		self.pool = multiprocessing.Pool(nworks)

	def partition(self,output_by_mapper):
		#mapper:[(key1,value1),(key2,value2),...]
		partition_data = collections.defaultdict(list)
		for key,value in output_by_mapper:
			print "@@@:\t",key,'\t',value
			partition_data[key].append(value)
		print "partition"
		return partition_data.items()

	def __call__(self,inputs,chunksize=1):
		#output_by_mapper = self.mapper(inputs)
		#output_by_partition = self.partition(output_by_mapper)
		#output_by_reducer = self.reducer(output_by_partition)
		print "inputs:\t",inputs
		output_by_mappers = self.pool.map(self.mapper,inputs,chunksize=chunksize)
		mapresult = itertools.chain(*output_by_mappers)
		partitioned_data = self.partition(itertools.chain(*output_by_mappers))
		reduced_value = self.pool.map(self.reducer,partitioned_data)
		return reduced_value


def mapper(input):
	print "mapper",input
	now = datetime.now()
	delta = timedelta(days=4)
	firsttime = (now -delta).strftime("%Y-%m-%d %H:%M:%S")


	ret = []
	with open(input) as inputfile:
		for oneline in inputfile:
			tmplist = []
			list_line = oneline.strip().split(' ')
			for element in list_line:
				if '=' in element:
					tmplist.append(element)
				else:
					tmplist[-1] = tmplist[-1] + ' ' + element

			time = client_ip = url = match_type = attack_type = rule_id = ''
			for e in tmplist:
				key_value = e.strip().split('=')
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

			if time < firsttime:
				continue

			event_type = ''
			if attack_type == '-':
				event_type = 'MODSEC_ALERT_EVENT_'+match_type.split('_')[-1]
			else:
				event_type = 'MODSEC_ALERT_EVENT_'+attack_type.split('_')[-1]
			ret.append((event_type,(time,client_ip,url,rule_id)))
	print ret
	return ret 


def reducer(inputs):
	print "reducer"
	#ba tongji de jieguo baocun dao shujukuzhong 
	#yi tiao yi tiao de tongji 
	return None 


if __name__ == "__main__":
	mapreduce = MapReduce(mapper,reducer)
	tmp = mapreduce(["data1","data2","data3"])
