#!/usr/bin/env python3

import sys
import subprocess
from collections import defaultdict
from bisect import bisect
import re
def int_ip(ip):
	byte_0, byte_1, byte_2, byte_3 = map(int, ip.split("."))
	dec = byte_0 << 24 | byte_1 << 16 | byte_2 << 8 | byte_3 << 0
	return dec


def main():	
	bottom_index = []
	countries = []
	countries_dict = defaultdict(lambda : len(countries_dict))
	for line in open("ip.csv"):
		values = re.match(r'"(\d+)","(\d+)","(.*)","(.*)"', line)
		bottom, top, code, name = values.groups()
		countries.append(countries_dict[name])
		bottom_index.append(int(bottom))
	countries_dict = {value : key for key, value in countries_dict.items()}			

	for line in sys.stdin:
		index = bisect(bottom_index, int_ip(line.split()[0]))
		print (countries_dict[countries[index - 1]] + "\t1")

if __name__ == "__main__":
	main()
