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
    Apr 16 10:00:27 127.0.0.1 proxy-server: 10.190.237.137 172.24.8.63 16/Apr/2017/10/00/27 DELETE /v1/MossoCloudFS_a504da1e-d009-423f-9b5a-b36b6c7ca280/ord-images_d1c/d1c71bdd-bb4d-4f22-8a35-7099edadd89d-00040 HTTP/1.0 204 - python-swiftclient-228.1.1.dev6 AACx1Rq_2r1SrDY1... - - - tx84d7bc39956040d99581b-0058f340bbord1 - 0.0849 - auth_user:novaopsord 1492336827.514173031 1492336827.599054098
    Apr 16 10:00:28 127.0.0.1 proxy-server: 23.253.124.251 172.24.8.30 16/Apr/2017/10/00/28 DELETE /v1/JungleDisk_prod_109846037/cfjd2-us-soccergaragecom1/BACKUPS/8202b92238322a8840e8c46e68c02f26/CHUNKS/0000029378 HTTP/1.0 404 - Jungle%20Disk%20Server%20Edition%20HTTP/86 XTzf2auHWa/m8yXb... - 70 - tx8e7f8b6d6df545c5bdeb8-0058f340bcord1 - 0.0391 - auth_user:JungleDisk_prod_109846037 1492336828.479855061 1492336828.518918037
    Apr 16 10:00:11 127.0.0.1 proxy-server: 10.190.237.137 172.24.8.65 16/Apr/2017/10/00/11 DELETE /v1/MossoCloudFS_a504da1e-d009-423f-9b5a-b36b6c7ca280/ord-images_078/07872aca-1720-4007-aa79-a65825b40721-00001 HTTP/1.0 204 - python-swiftclient-228.1.1.dev6 AACx1Rq_O8LkSEv9... - - - tx0d9e4767b708457a818e8-0058f340abord1 - 0.0945 - auth_user:novaopsord 1492336811.677512884 1492336811.772048950
    Apr 16 10:00:11 127.0.0.1 proxy-server: 2600:3c03::f03c:91ff:fe37:ded8 172.24.8.28 16/Apr/2017/10/00/11 PUT /v1/MossoCloudFS_1439270b-bfcf-412d-88d2-3a4d3adc08b9/NevMedia/aquamineral-catalog-old/mobile/style/icon/clear.png HTTP/1.0 201 - turbolift AACx1Rq_j_b4PJh9... 3051 - - txd751ace2845042d698ef7-0058f340abord1 - 0.1112 - auth_user:tzikag 1492336811.708946943 1492336811.820147991
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
        # print offset

if __name__ == "__main__":
    list_of_files = get_logs()
    log_files = list_of_files

    cbf = CustomBloomFilter(500000, 3)
    for item_file in log_files:
        all_objects = filter_logs(item_file)
        for item in all_objects:
            # print item
            cbf.insert(item)

    print cbf.lookup("Hello")
    print cbf.lookup("b732b2689c7e584afa75383840655a56")
    print cbf.lookup("9aab2bc5fba3ed3ba42802df1b332bad")
    print cbf.lookup("581ac41b3844d3076973ededdcdac009")
    print cbf.lookup("581ac41b3844d3076973ededdcdac") #False Case
    print cbf.lookup("ea18f2b812afb9192cb8a4fc9be74bc0")
    print cbf.lookup("ea18f2b812afb9192cb8a4fc9be74bbb") #False Case

