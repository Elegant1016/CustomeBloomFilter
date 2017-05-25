# Author: Heith S
# How to Execute(2.7.13): python custom_bloom.py
# Objective: Fast look up of the swift objects in the cluster

from StringIO import StringIO
from swiftly.client import StandardClient
import humanize
import os
import random
import gzip
from sarge import get_stdout
import tabulate
import datetime
import cPickle as pickle

# from custom_bloom import filter_logs, get_logs, CustomBloomFilter
from custom_bloom_filter import filter_logs, get_logs, CustomBloomFilter, lookup, SIZE_OF_BLOOMFILTER, NO_OF_HASH_FUNCTION

client = StandardClient(
    auth_url='https://swauth.ord1.swift.racklabs.com/auth/v1.0',
    auth_user='swiftlog:statsuser', auth_key='VHZmEKSJm6nNs', insecure=True)

'''
get_container(container, headers=None, prefix=None, delimiter=None, marker=None, end_marker=None, limit=None, query=None, cdn=False, decode_json=True)
GETs the container and returns the results. This is done to list the objects for the container.

Returns: A tuple of (status, reason, headers, contents).
status:	is an int for the HTTP status code.
reason:	is the str for the HTTP status (ex: "Ok").
headers:	is a dict with all lowercase keys of the HTTP headers; if a header has multiple values, it will be a list.
contents:	is the decoded JSON response or the raw str for the HTTP body.

JSON file

{"hash": "134d5a3c071c28e7317f3f9748447582", "last_modified": "2017-04-16T00:55:02.231420", "bytes": 881137, "name": "2017/04/16/00/011474479bba6c1d4336ae942be10a81.gz", "content_type": "application/octet-stream"},
  {"hash": "d5b64544f0eec4282bcca3627ec4eb42", "last_modified": "2017-04-16T00:55:02.773990", "bytes": 4863043, "name": "2017/04/16/00/0440daf8fd66a5d515cbccfc1cd8b770.gz", "content_type": "application/octet-stream"},
  {"hash": "0672be88415e4c1112c34c20151dabee", "last_modified": "2017-04-16T00:55:03.093690", "bytes": 873753, "name": "2017/04/16/00/0539bd70934df20f35f127d3d9fd6ef0.gz", "content_type": "application/octet-stream"},
  {"hash": "9d79c048ccf7ce4e12123c79e787de96", "last_modified": "2017-04-16T00:55:02.320840", "bytes": 877780, "name": "2017/04/16/00/0619ff21be807b0a51ef75d0e91b232e.gz", "content_type": "application/octet-stream"},
  {"hash": "46332e54b840e5063523815b5cf6bb86", "last_modified": "2017-04-16T00:55:03.454630", "bytes": 880980, "name": "2017/04/16/00/069ba1c0357ca27dee39b1785de35823.gz", "content_type": "application/octet-stream"},
  {"hash": "01d877da30300b302e82ace7a30241d7", "last_modified": "2017-04-16T00:55:02.371020", "bytes": 873628, "name": "2017/04/16/00/0827e30e0314cc45084a40a665c64f27.gz", "content_type": "application/octet-stream"},
  {"hash": "913fb31d8ef98d5ada7f5d7995f0dc6b", "last_modified": "2017-04-16T00:55:03.009810", "bytes": 883358, "name": "2017/04/16/00/0abb941fa3a16eade6bd031ef81a0217.gz", "content_type": "application/octet-stream"},
  {"hash": "684cbddfc6e7fa83170055452d17acd9", "last_modified": "2017-04-16T00:55:02.216090", "bytes": 6445282, "name": "2017/04/16/00/0b7c8ce52050cfc3fc1da8997e1fbd4d.gz", "content_type": "application/octet-stream"},
'''


def get_objects_by_date_range(start="2017/04/16/10", end="2017/04/16/11"):
    container = client.get_container("access_raw", marker=start, end_marker=end, decode_json=True)
    c = container[2]
    object_list = container[3]

    print "- " * 5
    print "-- Container Stats --"
    print "Total size of access_raw: {}".format(humanize.naturalsize(c["x-container-bytes-used"]))
    print "Total file count in access_raw: {}".format(humanize.intcomma(c["x-container-object-count"]))
    print "- " * 5
    print ""

    return sorted(object_list, key=lambda x: x["bytes"])


def _get_logs(obj_list):
    # names = [o['name'] for o in obj_list]
    '''
    {"hash": "0672be88415e4c1112c34c20151dabee", "last_modified": "2017-04-16T00:55:03.093690", "bytes": 873753, "name": "2017/04/16/00/0539bd70934df20f35f127d3d9fd6ef0.gz", "content_type": "application/octet-stream"}
    '''
    for i, n in enumerate(obj_list):
        download_log_by_name(n)

    print "Total logs in set: {}".format(len(obj_list))


def get_samples(obj_list, max_logs=5):
    if len(os.listdir("Logs")) >= max_logs:
        return os.listdir("Logs")

    while len(os.listdir("Logs")) < max_logs:
        print "getting samples..."
        random_file = random.choice(obj_list)
        download_log_by_name(random_file)

    return os.listdir("Logs"),


def download_log_by_name(obj_name):
    '''
    "name": "2017/04/16/00/0539bd70934df20f35f127d3d9fd6ef0.gz"
    '''
    _short_name = obj_name['name'].split(".gz")[0].split("/")[-1]

    if os.path.exists("Logs/{}.txt".format(_short_name)):
        return

    print "------ Getting log:  {}".format(obj_name)
    obj_tuple = client.get_object("access_raw", obj_name["name"], stream=False)
    with open('Logs/{}.txt'.format(_short_name), 'w') as out_file:
        f = gzip.GzipFile(mode='rb', fileobj=StringIO(obj_tuple[-1]))
        out_file.write(f.read())

if __name__ == "__main__":
    obj_list = get_objects_by_date_range()

    samples = _get_logs(obj_list=obj_list)

    log_files = get_logs()

    cbf = CustomBloomFilter(SIZE_OF_BLOOMFILTER, NO_OF_HASH_FUNCTION)

    for item_file in log_files:
        all_objects = filter_logs(item_file)
        for item in all_objects:
            # print item
            cbf.insert(item)

    cbf.persist_bloom(cbf.bit_array)

    start = datetime.datetime.now()
    list_of_objects = ["Hello", "b732b2689c7e584afa75383840655a56", "9aab2bc5fba3ed3ba42802df1b332bad", "581ac41b3844d3076973ededdcdac009", "581ac41b3844d3076973ededdcdac",
                       "ea18f2b812afb9192cb8a4fc9be74bc0", "ea18f2b812afb9192cb8a4fc9be74bbb", "Santosh"]

    for i in range(cbf.no_of_filters + 1):
        filename = "data{0}.pkl".format(i)
        pkl_file = open(filename, 'r')
        data1 = pickle.load(pkl_file)
        for item in list_of_objects:
            if (lookup(item, data1, cbf.hash_count, cbf.size)):
                print "Found in Bloom Filter {0}".format(filename)
            else:
                print "Not Found in Bloom Filter {0}".format(filename)
        pkl_file.close()
        print ""

    print "-done-"
    print "Number of Bloom Filters used: {0}".format(cbf.no_of_filters + 1)
