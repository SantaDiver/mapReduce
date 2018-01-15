#!/usr/bin/python

import sys
import pandas
from nltk.tokenize import RegexpTokenizer
import mincemeat
import csv
import argparse

RESULTS_DIRECTORY = './results/'

import os
directory = os.path.dirname(RESULTS_DIRECTORY)
try:
    os.stat(RESULTS_DIRECTORY)
except:
    os.mkdir(RESULTS_DIRECTORY)

PATH_TO_RESULT = RESULTS_DIRECTORY + 'Matrix.csv'
DATA_DIR = './matrix/'
PASSWROD = 'dontforgetme'

# Read data
parser = argparse.ArgumentParser()
parser.add_argument('-m', help='csv file with first matricies', required=True)
parser.add_argument('-n', help='matricies size', required=True)
args = parser.parse_args()

PATH_TO_M = DATA_DIR + args.m
N = int(args.n)

data = {}
key=0
with open(PATH_TO_M, 'r') as file:
    rows = csv.DictReader(file, delimiter=',')
    for row in rows:
        data[key] = row
        data[key]['n'] = N
        key += 1

# Server params
def mapfn(k, v):
    for i in range(v['n']):
        if v['matrix'] == 'A':
            yield (int(v['row']), i) , (int(v['col']), int(v['val']))
        else:
            yield (i, int(v['col'])) , (int(v['row']), int(v['val']))

def reducefn(key, vals):
    res = 0
    used = {}
    for val in vals:
        if val[0] in used:
            res += used[val[0]] * val[1]
        else:
            used[val[0]] = val[1]

    return res%97

server = mincemeat.Server()
server.mapfn = mapfn
server.reducefn = reducefn
server.datasource = data

print('Server is now running...')
result = server.run_server(password=PASSWROD)

# Write result
with open(PATH_TO_RESULT, 'w') as csvFile:
    csvFile.write(','.join(['matrix', 'row', 'col', 'val']) + '\n')
    for key,value in result.iteritems():
        csvFile.write('c,' + str(key[0]) + ',' + str(key[1]) + ',' + \
            str(value) + '\n')
