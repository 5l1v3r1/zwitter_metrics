#!/usr/bin/env python

import sys
import zwitter_request

new_users = {}
def is_new(date, ip):
	if date not in new_users.keys():
		new_users[date] = set(open("new_users/" + date).read().split())
	return ip in new_users[date]

def main():
	for line in sys.stdin:
		request = zwitter_request.ZWitterRequest(line)
		if request.valid() and is_new(request.date, request.ip):
			new_line = [request.ip, request.seconds, request.page, request.reference, request.date]
			print ("\t".join(map(str, new_line)))

if __name__ == "__main__":
	main()
