#!/usr/bin/python3

import sys
from math import sqrt


sum_11 = sum_22 = sum_12 = 0
current_pair = None
current_name = None

# input comes from STDIN
for line in sys.stdin:
    # split the input
    line = line.strip().split('\t')
    if len(line) == 3:
        current_name = line[-1]
        continue

    # parse the input we got from mapper_2.py
    movie1, movie2, rating1, rating2, mean_rating = line
    pair = movie1 + ':' + movie2

    try:
        rating1 = float(rating1)
        rating2 = float(rating2)
        mean_rating = float(mean_rating)
    except ValueError:
        sys.stderr.write('Illegel value')
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: pair) before it is passed to the reducer
    if current_pair == pair:
        sum_11 += (rating1 - mean_rating) * (rating1 - mean_rating)
        sum_22 += (rating2 - mean_rating) * (rating2 - mean_rating)
        sum_12 += (rating1 - mean_rating) * (rating2 - mean_rating)
    else:
        if current_pair:
            numerator = sum_12
            denominator = sqrt(sum_11) * sqrt(sum_22)

            score = 0.0
            if denominator:
                score = (numerator / (float(denominator)))

            if score < 0:
                score = 0.0
            # write result to STDOUT
            # movieA:movieB \t score
            current_pair = current_pair.split(':')
            print('%s\t%s\t%s\t%s' % (current_pair[0], current_pair[1], score, current_name))
        sum_11 = (rating1 - mean_rating) * (rating1 - mean_rating)
        sum_22 = (rating2 - mean_rating) * (rating2 - mean_rating)
        sum_12 = (rating1 - mean_rating) * (rating2 - mean_rating)
        current_pair = pair

# do not forget to output the last user if needed!
if current_pair == pair:
    numerator = sum_12
    denominator = sqrt(sum_11) * sqrt(sum_22)

    score = 0.0
    if denominator:
        score = (numerator / (float(denominator)))

    if score < 0:
        score = 0.0
    # write result to STDOUT
    # movieA:movieB \t score
    current_pair = current_pair.split(':')
    print('%s\t%s\t%s\t%s' % (current_pair[0], current_pair[1], score, current_name))
