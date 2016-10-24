#!/usr/bin/env python

import sys

def main():
	k = sys.argv[1]
	objs = []
	for line in sys.stdin:	
		objs.append(tuple(line.split()))
	objs.sort(key = lambda t: (-int(t[1]), t[0]))
	for obj in objs[:(int(k))]:
		print (obj[0] + '\t' + str(obj[1])) 

if __name__ == '__main__':
	main()

