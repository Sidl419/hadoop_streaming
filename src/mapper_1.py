#!/usr/bin/python3

import sys
import csv

# input comes from STDIN (standard input)
for line in csv.reader(sys.stdin, delimiter=',', quotechar='"'):
    # first line consists of labels
    
    ind = False
    for elem in line:
        ind |= elem.isdigit()
    if not ind:
        continue

    if len(line) != 4:
        # movieID, -2, movie_name
        print('%s\t%s\t%s' % (line[0], '-2', line[1]))
    else:
        # split the input
        userID, movieID, rating, timestamp = line

        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer_1.py
        #
        # tab-delimited; userID, movieID:rating
        print('%s\t%s' % (userID, movieID + ':' + rating))
