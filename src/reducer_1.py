#!/usr/bin/python3

import sys

current_movie = ''
current_user = None

# input comes from STDIN
for line in sys.stdin:
    line = line.strip().split('\t')
    if len(line) == 3:
        print('%s\t%s\t%s' % (line[0], line[1], line[2]))
        continue
    # parse the input we got from mapper_1.py
    user, movie_rating = line
    
    # this IF-switch only works because Hadoop sorts map output
    # by key (here: user) before it is passed to the reducer
    if current_user == user:
        current_movie += ',' + movie_rating
    else:
        if current_user:
            # write result to STDOUT
            # tab-delimited; userID, movie1:rating1,movie2:rating2, ... 
            print('%s\t%s' % (current_user, current_movie))
        current_movie = movie_rating
        current_user = user

# do not forget to output the last user if needed!
if current_user == user:
    # tab-delimited; userID, movie1:rating1,movie2:rating2, ... 
    print('%s\t%s' % (current_user, current_movie))
