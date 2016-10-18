#!/usr/bin/env python3

import sys
if __name__ == '__main__':
	total = 0
	for line in sys.stdin:
		_, count = line.split(' ')
		total += int(count)
	print ('hits', total)
