#!/usr/bin/env python

import sys
import zwitter_request

def main():
	fields = sys.argv[1:]
	for line in sys.stdin:
		request = zwitter_request.ZWitterRequest(line)
		if request.valid():
			new_line = []
			for field in fields:
				new_line.append(str(request.__dict__[field]))
			print ("\t".join(new_line))

if __name__ == "__main__":
	main()
