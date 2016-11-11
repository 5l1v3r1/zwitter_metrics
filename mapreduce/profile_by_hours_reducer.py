#!/usr/bin/env python

import sys
def main():
	old_key = None
	for line in sys.stdin:
		profile, hour, ip = line.split()
		if profile != old_key:
			if old_key is not None:
				for old_hour, ips in hours_dict.items():
					print  '\t'.join([old_key, old_hour, str(len(ips)), str(len(set(ips)))])
			old_key = profile
			hours_dict = {str(i).zfill(2) : [] for i in range(24)}
	
		hours_dict[hour].append(ip)		
		
	for old_hour, ips in hours_dict.items():
		print '\t'.join([old_key, old_hour, str(len(ips)), str(len(set(ips)))])

if __name__ == '__main__':
	main()

