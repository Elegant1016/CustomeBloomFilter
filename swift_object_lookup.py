# Author: Heith S
# How to Execute(2.7.13): python custom_bloom.py
# Objective: Fast look up of the swift objects in the cluster

from StringIO import StringIO
from swiftly.client import StandardClient
import os
import random
import gzip
import datetime
import cPickle as pickle
import sys

# from custom_bloom import filter_logs, get_logs, CustomBloomFilter
from custom_bloom_filter import filter_logs, get_logs, CustomBloomFilter, SIZE_OF_BLOOMFILTER, NO_OF_HASH_FUNCTION

client = StandardClient(
    auth_url='https://swauth.ord1.swift.racklabs.com/auth/v1.0',
    auth_user='swiftlog:statsuser', auth_key='VHZmEKSJm6nNs', insecure=True)


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
            cbf.insert(item)

    cbf.persist_bloom(cbf.bit_array)

    print "Number of Bloom Filters created: {0}".format(cbf.no_of_filters + 1)
    print "-done-"
