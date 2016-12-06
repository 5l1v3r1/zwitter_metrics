#!/usr/bin/env python

import sys
from pyspark import SparkContext
from pyspark import SparkConf
import datetime

import time
import re
import sys
import os

class ZWitterRequest:
	def __init__(self, line):
		values = re.match(r'([(\d\.)]+) - - \[(.*?) \+0400\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"', line)
		try:
			values = values.groups()
			self.ip = values[0]
			self.time = time.strptime(values[1], "%d/%b/%Y:%H:%M:%S")
			self.code = int(values[3])
			self.page = re.match(r'GET (.*) HTTP/1.1', values[2]).groups()[0]
			self.size = int(values[4])
			self.reference = values[5]
			self.user_agent = values[6]
			self.one = 1
			self.seconds = int(time.mktime(self.time))
			self.date = time.strftime("%Y-%m-%d", self.time)
		except:
			self.code = 404

	def valid(self):
		return self.code == 200

def get_profile(page, only_like):
	match_obj = None
	if only_like:
		match_obj = re.match(r'^.(id\d{5}).*like=1', page)
	else:
		match_obj = re.match(r'^.(id\d{5})', page)
	if match_obj is not None:
		return match_obj.group(1)
	else:
		return None		



def filter_profile(line):
	request = ZWitterRequest(line)
	if not request.valid():
			return []
	profile = get_profile(request.page, only_like = True)
	if profile is not None:
		return [(profile, request.date)]
	else:
		return []


def main():
	file_name_prefix = "hdfs://hadoop2-10.yandex.ru:8020/user/sandello/logs/access.log."
	#file_name_prefix = '/user/aseregin/sample'
	conf = SparkConf().setAppName("aseregin_likes")
	sc = SparkContext(conf=conf)
	date = datetime.datetime(*map(int, sys.argv[1].split('-')))
	for i_date in range(3):
		date_str = date.strftime('%Y-%m-%d')
		data = sc.textFile(file_name_prefix + date_str)
		if i_date == 0:
			lines = data	
		else:
			lines = lines.union(data)
		date = date - datetime.timedelta(days = 1)
	profiles = lines.flatMap(filter_profile)
	profiles_groped = profiles.groupByKey()

	liked_profiles = profiles_groped.filter(lambda x: len(set(x[1])) == 3)
	count =  liked_profiles.map(lambda x: ('1', 1)).reduceByKey(lambda x, y: x + y)
	count = count.map(lambda x: x[1])
	directory = os.path.join('profile_liked_three_days', sys.argv[1])
	
	count.saveAsTextFile(directory)

	sc.stop()


if __name__ == "__main__":
   main()
