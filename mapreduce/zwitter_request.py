import time
import re
import sys
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

	def from_facebook(self):
		return self.referece.find('facebook.com') != -1
	def valid(self):
		return self.code == 200
