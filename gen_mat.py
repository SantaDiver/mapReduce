import argparse
import csv
import os
import random

parser = argparse.ArgumentParser()
parser.add_argument('-size', help='maxrix size', required = True)
args= parser.parse_args()

matrix1 = 'A'
matrix2 = 'B'
size = int(args.size)
dirname = './matrix/'
filename = matrix1 + matrix2 + '.csv'

# random.seed(123456789)
with open(os.path.join(dirname, filename), 'w') as fout:
    w = csv.writer(fout, delimiter=',')
    w.writerow(['matrix', 'row', 'col', 'val'])
    for i in range(size):
        for j in range(size):
            w.writerow([matrix1, i, j, random.randint(0, 100)])
    for i in range(size):
        for j in range(size):
            w.writerow([matrix2, i, j, random.randint(0, 100)])
