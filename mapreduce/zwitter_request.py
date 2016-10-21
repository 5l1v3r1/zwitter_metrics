import time
import re
import sys
import subprocess
from collections import defaultdict
from bisect import bisect
class ZWitterRequest:
	def __init__(self, line):
		values = re.match(r'([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"', line)
		if values is None:
			print (line, file = sys.stderr)
			print (values, file = sys.stderr)
		values = values.groups()
		self.ip = values[0]
		self.time = time.strptime(values[1], "%d/%b/%Y:%H:%M:%S %z")
		self.code = int(values[3])
		self.page = re.match(r'GET (.*) HTTP/1.1', values[2]).groups()[0]
		self.size = int(values[4])
		self.reference = values[5]
		self.user_agent = values[6]
		self.one = 1
		self.seconds = int(time.mktime(self.time))
		self.date = time.strftime("%Y-%m-%d", self.time)
	
	Countries = None
	BottomIndex = None
	CountriesDict = None
		
	def country_name(self):
		if ZWitterRequest.Countries is None:
			ZWitterRequest.BottomIndex = []
			ZWitterRequest.Countries = []
			ZWitterRequest.CountriesDict = defaultdict(lambda : len(ZWitterRequest.CountriesDict))
			cat = subprocess.Popen(["hadoop", "fs", "-cat", "/user/sandello/dicts/IP2LOCATION-LITE-DB1.CSV"],
						 stdout=subprocess.PIPE)
			for line in cat.stdout:
				values = re.match(r'"(\d+)","(\d+)","(.*)","(.*)"', line.decode('utf-8').strip())
				bottom, top, code, name = values.groups()
				ZWitterRequest.Countries.append(self.CountriesDict[name])
				ZWitterRequest.BottomIndex.append(int(bottom))
			ZWitterRequest.CountriesDict = {value : key for key, value in ZWitterRequest.CountriesDict.items()}
		#print (ZWitterRequest.CountriesDict)			
		index = bisect(ZWitterRequest.BottomIndex, self.int_ip())
		return ZWitterRequest.CountriesDict[ZWitterRequest.Countries[index - 1]] 
	
	def int_ip(self):
		byte_0, byte_1, byte_2, byte_3 = map(int, self.ip.split("."))
		dec = byte_0 << 24 | byte_1 << 16 | byte_2 << 8 | byte_3 << 0
		return dec

	def valid(self):
		return self.code == 200
