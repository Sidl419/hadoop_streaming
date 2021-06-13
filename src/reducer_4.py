#!/usr/bin/python3

import sys

current_movie = ''
current_user = None
current_count = 0

# input comes from STDIN
for line in sys.stdin:
    # parse the input we got from mapper_4.py
    user_rating, movie = line.strip().split('\t')
    user, rating = user_rating.split(':')
    movie_rating = rating + '#' + movie
    
    # this IF-switch only works because Hadoop sorts map output
    # by key (here: user) before it is passed to the reducer
    if (current_user == user):
        if current_count < 100:
            current_movie += '@' + movie_rating
        current_count += 1
    else:
        if current_user:
            # write result to STDOUT
            #userID@rating1#movie1@rating2#movie2@...@rating100#movie100
            print('%s@%s' % (current_user, current_movie))
        current_movie = movie_rating
        current_user = user
        current_count = 0

# do not forget to output the last user if needed!
if current_user == user:
    #userID@rating1#movie1@rating2#movie2@...@rating100#movie100
    print('%s@%s' % (current_user, current_movie))
