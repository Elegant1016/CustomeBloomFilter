# Author: R Santosh
# How to Execute(2.7.13): python custom_bloom.py
# Objective: Fast look up of the swift objects in the cluster
#
# References : https://brilliant.org/wiki/bloom-filter/
#
# 1.Reading each file the Logs folder and collection the Swift Object
# 2.Calculating the md5 digest of the object and then
# 3.Storing/Inserting the object's md5 digest in the Bloom Filter.(Uses Murmur hash function to fill the bit array)
# 4.The object is searched using the lookup method of the bloom filter.

import os
import datetime
from hashlib import md5
from bitarray import bitarray
import mmh3
import cPickle as pickle
import sys

SIZE_OF_BLOOMFILTER = 500000 # Bloom filter of this size
NO_OF_HASH_FUNCTION = 3      # Different Hash function used
ERROR_TOLERANCE = .20

class SampleObj(object):
    def __init__(self, name, arr):
        self.name = name
        self.arr = arr


class CustomBloomFilter:
    def __init__(self, size, hash_count):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = bitarray(size)
        self.bit_array.setall(0)
        self.bit_occupied = 0
        self.prob = 0
        self.no_of_filters = 0

    def get_tolerance(self):
        return ERROR_TOLERANCE

    def insert(self, string):
        if self.prob > self.get_tolerance():
            #1. Persist the bloom Filter
            self.persist_bloom(self.bit_array)

            #2.Reset the values, bit_occupied and probabilities
            self.bit_array.setall(0)
            self.bit_occupied = 0
            self.prob = 0
            self.no_of_filters += 1

        for seed in range(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            self.bit_array[result] = 1
        self.bit_occupied += 1
        self.prob = (1.0 - ((1.0 - 1.0/self.size)**(self.hash_count*self.bit_occupied))) ** self.hash_count

    def persist_bloom(self, bit_array):
        filename = "data{0}.pkl".format(self.no_of_filters)
        output = open(filename, 'w')
        pickle.dump(bit_array, output)
        output.close()

    def __contains__(self, string):
        """
        Check if a key is a member of the bloom filter using the "in" operator.
        """
        for seed in range(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            # print "Hello :", result

            if self.bit_array[result] == 0:
                return False
        return True

def get_logs():
    """ This is just a mock, no need to make it complex"""
    list_of_files = []
    for dirname, subdir, filenames in os.walk("Logs"):
        for filename in os.listdir(dirname):
            if filename.endswith(".txt"):
                abs_path = "%s/%s" % (dirname, filename)
                list_of_files.append(abs_path)
    return list_of_files



def read_in_chunks(file_object, chunk_size=65536):
    while True:
        data = file_object.readlines(chunk_size)
        if not data:
            break
        yield data

def filter_logs(logs):
    """ A generator that yields a es formatted dictionary for matching lines
    :param logs: A list of log files to process
    """
    fd = open(logs)
    index = 0
    offset = 0

    for chunk in read_in_chunks(fd):
        for ln in chunk:
            try:
                verb, obj, _, status = ln.split()[8:12]
            except ValueError:
                continue
            if verb in ("DELETE", "PUT"):
                # print(verb)
                # print((obj[4:]))
                yield md5((obj[4:]).encode('utf-8')).hexdigest()

        offset = index + len(chunk)  # increasing the offset
        index = offset

def lookup(string, bit_array, hash_count, size):
    for seed in range(hash_count):
        result = mmh3.hash(string, seed) % size
        if bit_array[result] == 0:
            return False
    return True

def calculate_md5code(test_obj):
    return md5(test_obj.encode('utf-8')).hexdigest()
