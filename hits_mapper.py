#!/usr/bin/env python3

import sys
import time
import re
import zwitter_request

if __name__ == "__main__":	
	for line in sys.stdin:
		request = zwitter_request.ZWitterRequest(line)
		if request.valid():
			print ("hits", 1)
