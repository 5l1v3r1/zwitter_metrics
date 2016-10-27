#!/usr/bin/env python

import sys
import datetime

def main():
	old_key = None
	command = sys.argv[1] #new, lost
	current_date = sys.argv[2]
	first_date = datetime.datetime(*map(int, current_date.split('-'))) - datetime.timedelta(days = 13)
	first_date = first_date.strftime("%Y-%m-%d")	
	dates_list = []
	list_equal_to_element = lambda l, d: len(l) == 1 and l[0] == d
	for line in sys.stdin:
		key, date = line.strip().split()
		if key != old_key:
			if old_key is not None:
				if  list_equal_to_element(dates_list, current_date if command == 'new' else first_date):
					print (old_key)
			old_key = key
			dates_list = []
		dates_list.append(date)

	if  list_equal_to_element(dates_list, current_date if command == 'new' else first_date):
		print (old_key)

if __name__ == "__main__":
	main()	
