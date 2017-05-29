# Author: R Santosh
# How to Execute(2.7.13): python test_lookup.py objName1 objName2
# Objective: Fast look up of the swift objects in the cluster

from custom_bloom_filter import lookup, calculate_md5code, SIZE_OF_BLOOMFILTER, NO_OF_HASH_FUNCTION
import cPickle as pickle
from hashlib import md5
import sys
import mmh3
import subprocess


if __name__ == '__main__':
    list_of_objects = []

    if len(sys.argv) >= 2:
        for i in range(1, len(sys.argv)):
            list_of_objects.append(calculate_md5code(str(sys.argv[i])))

        cmd = "ls *.pkl | wc -l"
        no_of_pkls = int(subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read())

        for i in range(no_of_pkls):
            filename = "data{0}.pkl".format(i)
            pkl_file = open(filename, 'r')
            data1 = pickle.load(pkl_file)
            for item in list_of_objects:
                # print item
                if (lookup(item, data1, NO_OF_HASH_FUNCTION, SIZE_OF_BLOOMFILTER)):
                    print "Found in {0}".format(filename)
                else:
                    print "Not Found in {0}".format(filename)
            pkl_file.close()
            print ""
