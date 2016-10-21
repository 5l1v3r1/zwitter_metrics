#!/usr/bin/env python3

import sys

def main():
	old_key = None
	if len(sys.argv) == 1:
		to_add = ""
	else:
		to_add = " " + sys.argv[1]
	for line in sys.stdin:
		key = line.strip()
		if key != old_key:
			print (key + to_add)
			old_key = key
		
if __name__ == "__main__":
	main()	
