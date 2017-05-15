# Author: R Santosh
# How to Eecxute(2.7.13): python custom_bloom.py
# Objective: Fast look up of the swift objects in the cluster
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


class CustomBloomFilter:
    def __init__(self, size, hash_count):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = bitarray(size)
        self.bit_array.setall(0)

    def insert(self, string):
        for seed in range(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            self.bit_array[result] = 1

    def lookup(self, string):
        for seed in range(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            if self.bit_array[result] == 0:
                return "Object not present"

        return "Probably, Object can be found"

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
            if verb in ("DELETE"):
                # print(verb)
                # print((obj[4:]))
                yield md5((obj[4:]).encode('utf-8')).hexdigest()

        offset = index + len(chunk)  # increasing the offset
        index = offset

if __name__ == "__main__":
    list_of_files = get_logs()
    log_files = list_of_files

    cbf = CustomBloomFilter(500000, 3)
    for item_file in log_files:
        all_objects = filter_logs(item_file)
        for item in all_objects:
            cbf.insert(item)

    print cbf.lookup("Hello")
    print cbf.lookup("b732b2689c7e584afa75383840655a56")
    print cbf.lookup("9aab2bc5fba3ed3ba42802df1b332bad")
    print cbf.lookup("581ac41b3844d3076973ededdcdac009")
    print cbf.lookup("581ac41b3844d3076973ededdcdac") #False Case
    print cbf.lookup("ea18f2b812afb9192cb8a4fc9be74bc0")
    print cbf.lookup("ea18f2b812afb9192cb8a4fc9be74bbb") #False Case

