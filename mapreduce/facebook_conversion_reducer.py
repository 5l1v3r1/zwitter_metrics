#!/usr/bin/env python3

import sys
import datetime

def main():
	old_key = None
	command = sys.argv[1] #signup, all
	current_date = sys.argv[2]
		
	dates_list = []
	from_facebook = False
	has_sign_up = False
	has_sign_up_today = False
	for line in sys.stdin:
		key, _, page, ref, date = line.strip().split()
		if key != old_key:
			if old_key is not None:
				if  from_facebook and (command == 'all' or (not has_sign_up and has_sign_up_today)):
					print (old_key)
			old_key = key
			from_facebook = ref.find('facebook') != -1
		if page.startswith('/signup'):
			has_sign_up = has_sign_up or date != current_date
			has_sign_up_today = date == current_date
	
	if  from_facebook and (command == 'all' or (not has_sign_up and has_sign_up_today)):
		print (old_key)
	
if __name__ == "__main__":
	main()	
