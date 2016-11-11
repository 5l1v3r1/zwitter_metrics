#!/usr/bin/env python
import happybase
import sys
import datetime
from time import mktime
import random

TABLE_NAME_PREFIX = 'bigdatashad_aseregin_'

def create_table(connection, table_name):
	if table_name in connection.tables():
		return connection.table(table_name)
	if table_name.endswith('profile_last_three_liked_users'):
		connection.create_table(table_name, {"cf" : dict(max_versions=3)})
	else:
		connection.create_table(table_name, {"cf" : dict()})
	return connection.table(table_name)
		
def put_key_value(table, key, value):
	table.put(unicode(key).encode('utf-8'), {"cf:value" : unicode(value).encode('utf-8')})
	
def profile_hits_and_users(conn, date):
	hits_table = create_table(conn, TABLE_NAME_PREFIX + 'profile_hits')
	users_table = create_table(conn, TABLE_NAME_PREFIX + 'profile_users')
	hits_batch = hits_table.batch(batch_size = 10000)
	users_batch = users_table.batch(batch_size = 10000)
	for line in sys.stdin:
		profile, hour, hits, users = line.split()
		key = '_'.join([date, profile, hour])
		put_key_value(hits_batch, key, hits)
		put_key_value(users_batch, key, users)
	hits_batch.send()
	users_batch.send()
			
def user_most_visited_profiles(conn, date):
	table = create_table(conn, TABLE_NAME_PREFIX + 'user_most_visited_profiles')
	batch = table.batch(batch_size = 10000)
	for line in sys.stdin:
		ip_profile, count = line.split()
		ip, profile = ip_profile.split('_')
		count = str(10 ** 12 - int(count))
		put_key_value(batch, '_'.join([date, ip, count.zfill(12), profile]),'id' + profile)

	batch.send()

def profile_last_three_liked_users(conn, date):
	table = create_table(conn, TABLE_NAME_PREFIX + 'profile_last_three_liked_users')
	date = datetime.datetime(*map(int, date.split('-')))
	timestamp = mktime(date.timetuple())
	batches = [table.batch(batch_size = 10000, timestamp = int(timestamp) + i) for i in range(3)]
	old_profile = None
	
	for line in sys.stdin:
		profile, _, ip = line.split()
		if profile != old_profile:
			slots_left = 3
		old_profile = profile			
		if slots_left != 0:
			slots_left -= 1
			for days_to_add in range(5):
				new_date = date + datetime.timedelta(days = days_to_add)
				new_date = new_date.strftime('%Y-%m-%d')
				put_key_value(batches[slots_left], new_date + '_' + profile, ip)

	for i in range(len(batches)):
		batches[i].send()		 

def main():
	HOSTS = ["hadoop2-%02d.yandex.ru" % i for i in xrange(11, 14)]
	host = random.choice(HOSTS)
	conn = happybase.Connection(host)
	command = sys.argv[1]
	date = sys.argv[2]
	globals()[command](conn, date)

if __name__ == "__main__":
	main()
			
