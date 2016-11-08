import argparse
import datetime
import getpass
import hashlib
import random
import struct
import re
from flask import Flask, request, abort, jsonify
import os

app = Flask(__name__)
app.secret_key = "kkkeeeyyy"


def iterate_between_dates(start_date, end_date):
    span = end_date - start_date
    for i in xrange(span.days + 1):
        yield start_date + datetime.timedelta(days=i)


@app.route("/")
def index():
    return "OK!"

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
		("facebook_signup_conversion_3",float)]
@app.route("/api/hw1")
def api_hw1():
    start_date = request.args.get("start_date", None)
    end_date = request.args.get("end_date", None)
    if start_date is None or end_date is None:
        abort(400)
    start_date = datetime.datetime(*map(int, start_date.split("-")))
    end_date = datetime.datetime(*map(int, end_date.split("-")))

    result = {}
    for date in iterate_between_dates(max(start_date, datetime.datetime(2016,10,07)), end_date):
	date_str = date.strftime("%Y-%m-%d")
	result[date_str] = {}
        for metric, date_type in metrics:
		filename = '/home/aseregin/hw1/result/' + metric + '/' + date_str
		if not os.path.isfile(filename):
			result[date_str][metric] = 0
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

