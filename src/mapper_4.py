#!/usr/bin/python3

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # split the input
    movie, value = line.strip().split('\t')
    user, rating = value.split(':')

    try:
        rating = round(float(rating), 1)
    except ValueError:
        # rating was not a number, so silently
        # ignore/discard this line
        sys.stderr.write(f'Illegel value: {rating}')

    # write the results to STDOUT (standard output);
    # what we output here will be the input for the
    # Reduce step, i.e. the input for reducer_1.py
    #
    # tab-delimited; userID, movieID:rating
    print('%s\t%s' % (user + ':' + str(rating), movie))
