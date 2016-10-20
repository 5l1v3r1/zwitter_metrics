#!/usr/bin/env python3
import sys
def main():
	for line in sys.stdin:
		if int(line.split()[2]) == 1:
			print (1, 1)
if __name__ == "__main__":
	main()
