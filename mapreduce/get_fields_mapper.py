#!/usr/bin/env python3

import sys
import zwitter_request

def main():
	fields = sys.argv[1:]
	for line in sys.stdin:
		request = zwitter_request.ZWitterRequest(line)
		if request.valid():
			new_line = []
			for field in fields:
				if field in request.__dict__:
					new_line.append(str(request.__dict__[field]))
				elif field in request.__class__.__dict__:
					new_line.append(str(request.__class__.__dict__[field](request)))
				else:
					assert False
			print ("\t".join(new_line))

if __name__ == "__main__":
	main()
