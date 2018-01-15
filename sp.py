#!/usr/bin/python

import sys
import pandas
from nltk.tokenize import RegexpTokenizer
import mincemeat

RESULTS_DIRECTORY = './results/'

import os
directory = os.path.dirname(RESULTS_DIRECTORY)
try:
    os.stat(RESULTS_DIRECTORY)
except:
    os.mkdir(RESULTS_DIRECTORY)

PATH_TO_DATA = './southpark/All-seasons.csv'
PATH_TO_RESULT = RESULTS_DIRECTORY + 'SouthPark.csv'
PASSWROD = 'dontforgetme'

# Read data
token = RegexpTokenizer(r'\w+')
pairs = [( pair[0], token.tokenize(pair[1].lower()) ) \
    for pair in pandas.read_csv(PATH_TO_DATA)[['Character', 'Line']].values]

# Server params
server = mincemeat.Server()
server.mapfn = lambda _, arr: (yield arr[0], set(arr[1]))
server.reducefn = lambda _, arr: len(set().union(*arr))
server.datasource = dict(enumerate(pairs))

print('Server is now running...')
result = server.run_server(password=PASSWROD)

# Write result
with open(PATH_TO_RESULT, 'w') as csvFile:
    csvFile.write('Character, Language Wealth\n')
    for name, words in result.iteritems():
        csvFile.write(name + ',' + str(words) + '\n')
