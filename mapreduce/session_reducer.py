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
				print ('\t'.join(map(str, (1, session_end - session_start, session_length, int(session_length == 1)))))
			session_start = seconds 
			session_length = 0
			old_ip = ip
		session_length += 1
		session_end = seconds
	print ('\t'.join(map(str, (1, session_end - session_start, session_length, int(session_length == 1)))))
	

if __name__ == '__main__':
	main()

