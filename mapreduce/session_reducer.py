#!/usr/bin/env python3

import sys

session_break = 30 * 60
def main():
	old_ip = None
	session_start = None
	session_end = None
	session_length = 0
	for line in sys.stdin:
		ip, seconds = line.split()		
		seconds = int(seconds)
		if ip != old_ip or session_end + session_break < seconds:
			if old_ip is not None:
				print (old_ip, session_end - session_start, session_length)
			session_start = seconds 
			session_length = 0
			old_ip = ip
		session_length += 1
		session_end = seconds
	print (old_ip, session_end - session_start, session_length)

if __name__ == '__main__':
	main()

