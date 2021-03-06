import argparse
import datetime
import getpass
import hashlib
import random
import struct
import re
from flask import Flask, request, abort, jsonify
import os
import happybase

app = Flask(__name__)
app.secret_key = "kkkeeeyyy"

def iterate_between_dates(start_date, end_date):
    span = end_date - start_date
    for i in xrange(span.days + 1):
        yield start_date + datetime.timedelta(days=i)


@app.route("/")
def index():
    return "OK!"


HOSTS = ["hadoop2-%02d.yandex.ru" % i for i in xrange(11, 14)]
host = random.choice(HOSTS)
 
TABLE_PREFIX = 'bigdatashad_aseregin_'
def get_dates_generator_from_request(request):
    start_date = request.args.get("start_date", None)
    end_date = request.args.get("end_date", None)
    if start_date is None or end_date is None:
        abort(400)
    start_date = datetime.datetime(*map(int, start_date.split("-")))
    end_date = datetime.datetime(*map(int, end_date.split("-")))
    return iterate_between_dates(max(start_date, datetime.datetime(2016,10,07)), end_date)

def get_result_for_hw2(request, key_name, get_method, default_val = []):
	key = request.args.get(key_name, None)
	if key is None:
		abort(400)
	result = {}
	for date in get_dates_generator_from_request(request):
		date_str = date.strftime("%Y-%m-%d")
		prefix = unicode(date_str + '_' + key).encode('utf-8')
		
		try:	
			result[date_str] = get_method(prefix)
		except:
			result[date_str] = []
		if len(result[date_str]) == 0:
			result[date_str] = default_val
	return jsonify(result)

@app.route("/api/hw2/profile_hits")
def api_hw2_profile_hits():
	conn = happybase.Connection(host)
	table = conn.table(TABLE_PREFIX + 'profile_hits')
	res = get_result_for_hw2(request, 'profile_id', lambda prefix: [int(value[b'cf:value'].decode('utf-8')) for key, value in table.scan(row_prefix = prefix)], [0] * 24)
	conn.close()
	return res

@app.route("/api/hw2/profile_users")
def api_hw2_profile_users():
	conn = happybase.Connection(host)
	table = conn.table(TABLE_PREFIX + 'profile_users')
	res = get_result_for_hw2(request, 'profile_id', lambda prefix: [int(value[b'cf:value'].decode('utf-8')) for key, value in table.scan(row_prefix = prefix)], [0] * 24)
	conn.close()
	return res

@app.route("/api/hw2/user_most_visited_profiles")
def api_hw2_user_most_visited_profiles():
	conn = happybase.Connection(host)
	table = conn.table(TABLE_PREFIX + 'user_most_visited_profiles')
	res = get_result_for_hw2(request, 'user_ip', lambda prefix: [value[b'cf:value'].decode('utf-8') for key, value in table.scan(row_prefix = prefix)])
	conn.close()
	return res




@app.route("/api/hw2/profile_last_three_liked_users")
def api_hw2_profile_last_three_liked_users():
	conn = happybase.Connection(host)
	table = conn.table(TABLE_PREFIX + 'profile_last_three_liked_users')
	res = get_result_for_hw2(request, 'profile_id', lambda prefix: [value.decode('utf-8') for value in table.cells(row = prefix, column = 'cf:value')])
	conn.close()
	return res




string_list = lambda x: x.split()


def dict_string_int (content):
	res = {}
	for line in content.splitlines():
		val, count = re.match(r"(.*)\s(\d*)", line.strip()).groups()
		res[val] = int(count)
	return res
metrics = [("total_hits", int), ("total_users", int), ("top_10_pages", string_list),
		("average_session_time", float), ("average_session_length", float), ("bounce_rate", float),
		("users_by_country", dict_string_int),
		("new_users", int), ("lost_users", int),
		("facebook_signup_conversion_3", float),
		('profile_liked_three_days', int)]
@app.route("/api/hw1")
def api_hw1():
    result = {}
    for date in get_dates_generator_from_request(request):
	date_str = date.strftime("%Y-%m-%d")
	result[date_str] = {}
        for metric, date_type in metrics:
		filename = '/home/aseregin/hw/hw1/result/' + metric + '/' + date_str
		if not os.path.isfile(filename):
			continue
		with open(filename) as f:
			print metric
			content = f.read()
			result[date_str][metric] = date_type(content)
		

    return jsonify(result)


def login_to_port(login):
    """
    We believe this method works as a perfect hash function
    for all course participants. :)
    """
    hasher = hashlib.new("sha1")
    hasher.update(login)
    values = struct.unpack("IIIII", hasher.digest())
    folder = lambda a, x: a ^ x + 0x9e3779b9 + (a << 6) + (a >> 2)
    return 10000 + reduce(folder, values) % 20000


def main():
    parser = argparse.ArgumentParser(description="HW 1 Example")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=login_to_port(getpass.getuser()))
    parser.add_argument("--debug", action="store_true", dest="debug")
    parser.add_argument("--no-debug", action="store_false", dest="debug")
    parser.set_defaults(debug=False)

    args = parser.parse_args()
  
    app.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == "__main__":
    main()

