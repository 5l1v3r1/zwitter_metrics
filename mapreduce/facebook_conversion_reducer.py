#!/usr/bin/env python

import sys
import datetime

def main():
	old_key = None
	current_date = sys.argv[1]
		
	from_facebook = False
	first_sign_up_date = None
	for line in sys.stdin:
		key, _, page, ref, date = line.strip().split()
		if key != old_key:
			if old_key is not None:
				if  from_facebook:
					print ("1\t" + str(int(first_sign_up_date == current_date)))
			old_key = key
			from_facebook = ref.find('facebook.com') != -1
			first_sign_up_date = None
		if page.strip() == '/signup' and first_sign_up_date is None:
			first_sign_up_date = date
	if  from_facebook:
		print ("1\t" + str(int(first_sign_up_date == current_date)))
	
	
if __name__ == "__main__":
	main()	
