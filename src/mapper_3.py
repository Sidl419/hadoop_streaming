#!/usr/bin/python3

import sys

movies = set()

user_movies = dict()

# input comes from STDIN (standard input)
for line in sys.stdin:
    try:
        # split the input
        line = line.strip().split('\t')

        if len(line) == 2:
            # calculate each user rating list: <movieA, movieB>
            user = line[0]
            movie_ratings = line[1].strip().split(',')

            watched_movies = set()
            
            for pair in movie_ratings:
                pair = pair.strip().split(':')
                movies.add(pair[0])
                watched_movies.add(pair[0])

            user_movies[user] = (line[1], watched_movies)
        else:
            print('%s\t%s\t%s\t%s\t%s' % (line[0], '-1', line[1], line[2], line[3]))
    except:
        pass

for user_t, (movie_ratings_t, watched_movies_t) in user_movies.items():
    try:
        for unwatched in (movies - watched_movies_t):
            print('%s\t%s\t%s\t%s\t%s' % (unwatched, user_t, movie_ratings_t, '-1', '-1'))
    except:
        pass
