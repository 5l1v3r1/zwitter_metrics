#!/usr/bin/env python

import sys

def main():
	old_key = None
	date = sys.argv[1]
	for line in sys.stdin:
		key = line.strip()
		if key != old_key:
			print (key + '\t' + date)
			old_key = key
		
if __name__ == "__main__":
	main()	
