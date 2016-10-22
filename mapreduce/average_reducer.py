#!/usr/bin/env python3

import sys

def main():
	total = None

	for line in sys.stdin:
		values = list(map(float, line.split()))
		if total is None:
			total = values
		else:
			for i in range(len(values)):
				total[i] += values[i]
	print ('\t'.join(map(lambda x: str(x / total[0]), total[1:])))
		
if __name__ == '__main__':
	main()

