#!/usr/bin/env python

import sys
import datetime

def main():
	old_key = None
	current_date = sys.argv[1]
		
	from_facebook = False
	first_sign_up_date = '9999-99-99'
	for line in sys.stdin:
		key, _, page, ref, date = line.strip().split()
		if key != old_key:
			if  from_facebook:
				print ("1\t" + str(int(first_sign_up_date == current_date)))
			old_key = key
			from_facebook = ref.find('facebook.com') != -1
			first_sign_up_date = '9999-99-99'
		if page.strip() == '/signup':
			first_sign_up_date = min(date, first_sign_up_date)
	if  from_facebook:
		print ("1\t" + str(int(first_sign_up_date == current_date)))
	
	
if __name__ == "__main__":
	main()	
