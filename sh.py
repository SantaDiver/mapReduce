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

PATH_TO_DATA = './sherlock'
PATH_TO_RESULT = RESULTS_DIRECTORY + 'Sherlock.csv'
PASSWROD = 'dontforgetme'

# Read data
data = {}
book_names = os.listdir(PATH_TO_DATA)
token = RegexpTokenizer(r'\w+')
for book_name in book_names:
    path_to_book = os.path.join(PATH_TO_DATA, book_name)
    with open(path_to_book, "r") as book:
        data[book_name] = token.tokenize(book.read().lower())

# Server params
def mapfn(book, text):
    for word in text:
        yield word, book

def reducefn(word, books):
    from collections import Counter
    return dict(Counter(books).items())

server = mincemeat.Server()
server.mapfn = mapfn
server.reducefn = reducefn
server.datasource = data

print('Server is now running...')
result = server.run_server(password=PASSWROD)

# Write result
with open(PATH_TO_RESULT, 'w') as csvFile:
    csvFile.write(',' + ','.join(book_names) + '\n')
    for word, books in result.iteritems():
        books_cnt = [str(books.get(name, 0)) for name in book_names]
        csvFile.write(word + ',' + ','.join(books_cnt) + '\n')
