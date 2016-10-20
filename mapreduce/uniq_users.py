#!/usr/bin/env python3

import sys
import zwitter_request

def mapper():
	for line in sys.stdin:
		request = zwitter_request.ZWitterRequest(line)
		if request.valid():
			print (request.ip)
def reducer():
	current_key = ""
	for line in sys.stdin:
		ip = line.strip()
		if ip != current_key:
			print (ip)
			current_key = ip
		
if __name__ == "__main__":
	command = sys.argv[1]
	eval(command + "()")
	
