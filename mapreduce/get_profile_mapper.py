#!/usr/bin/env python

import sys
import zwitter_request
import re
def get_profile(page, only_like):
	match_obj = None
	if only_like:
		match_obj = re.match(r'^.(id\d{5}).*like=1', page)
	else:
		match_obj = re.match(r'^.(id\d{5})', page)
	if match_obj is not None:
		return match_obj.group(1)
	else:
		return None		

def main():
	only_like = int(sys.argv[1])
	fields = sys.argv[2:]
	for line in sys.stdin:
		request = zwitter_request.ZWitterRequest(line)
		request.profile = get_profile(request.page, only_like)
		request.hour = str(request.time.tm_hour).zfill(2)
		request.ip_profile = request.ip + '_' + str(request.profile)
		if request.valid() and request.profile is not None:
			new_line = []
			for field in fields:
				new_line.append(str(request.__dict__[field]))
			print ("\t".join(new_line))

if __name__ == "__main__":
	main()
