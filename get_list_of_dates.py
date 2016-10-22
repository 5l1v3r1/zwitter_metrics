#!/usr/bin/env python3
import subprocess
import sys
from datetime import datetime, timedelta
def main():
	date = sys.argv[1]
	window = int(sys.argv[2])
	result = []
	for delta in map(lambda x: timedelta(days = x), range(window)):
		d = (datetime(*map(int, date.split('-'))) - delta).strftime("%Y-%m-%d")
		code = subprocess.call(["hdfs", "dfs", "-test","-e","/user/sandello/logs/access.log." + d])
		if code == 0:
			result.append(d)
	print ('\t'.join(result))

if __name__ == "__main__":
	main()
