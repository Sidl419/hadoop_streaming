#!/usr/bin/python3

import sys

sim = dict()
current_name = None

current_unwatched = None
unwatched_dict = dict()

# input comes from STDIN
for line in sys.stdin:
    # parse the input we got from mapper_3.py
    line = line.strip().split('\t')
        
    # differ two data sources
    if line[1] != '-1':
        user = line[1]
        unwatched = line[0]

    else:
        # convert value (currently a string) to float
        try:
            value = float(line[3])
        except ValueError:
            # value was not a number, so silently
            # ignore/discard this line
            sys.stderr.write(f'Illegel value: {line[3]}')
            continue

        sim[line[2]] = value
        current_name = line[4]
        continue
    
    # this IF-switch only works because Hadoop sorts map output
    # by key (here: unwatched) before it is passed to the reducer
    if current_unwatched == unwatched:
        unwatched_dict[user] = line[2]
    else:
        if current_unwatched:
            for userID, watched_movies in unwatched_dict.items():

                numerator = 0
                denominator = 0

                for movie_rating in watched_movies.split(','):
                    movie, rating = movie_rating.split(':', 1)
                    
                    # convert rating (currently a string) to float
                    try:
                        rating = float(rating)
                    except ValueError:
                        # rating was not a number, so silently
                        # ignore/discard this line
                        sys.stderr.write(f'Illegel value: {rating}')

                    try:
                        denominator += sim[movie]
                        numerator += sim[movie] * rating
                    except:
                        pass
                        
                unwatched_rating = 0.0
                if denominator:
                    unwatched_rating = (numerator / (float(denominator)))

                print('%s\t%s' % (current_name, userID + ':' + str(unwatched_rating)))
                
        current_unwatched = unwatched
        unwatched_dict = dict()
        unwatched_dict[user] = line[2]

# do not forget to output the last user if needed!
if current_unwatched == unwatched:
     for userID, watched_movies in unwatched_dict.items():
        numerator = 0
        denominator = 0

        for movie_rating in watched_movies.split(','):
            movie, rating = movie_rating.split(':', 1)
                    
            # convert rating (currently a string) to float
            try:
                rating = float(rating)
            except ValueError:
                # rating was not a number, so silently
                # ignore/discard this line
                sys.stderr.write(f'Illegel value: {rating}')

            try:
                denominator += sim[movie]
                numerator += sim[movie] * rating
            except:
                pass
                        
        unwatched_rating = 0.0
        if denominator:
            unwatched_rating = (numerator / (float(denominator)))

        print('%s\t%s' % (current_name, userID + ':' + str(unwatched_rating)))