#!/usr/bin/python3

import sys
from itertools import combinations

# input comes from STDIN (standard input)
for line in sys.stdin:
	# split the input
	line = line.strip().split('\t')
	if len(line) == 3:
		print('%s\t%s\t%s' % (line[0], '-1', line[2]))
		continue

	# calculate each user rating list: <movieA, movieB>
	movie_ratings = line[1].split(',')

	# calculate mean rating per user
	mean_rating = 0

	for pair in movie_ratings:
		pair = pair.split(':')
		rating = pair[1]
		movie = pair[0]

		try:
			rating = float(rating)
		except ValueError:
			# rating was not a number, so silently
			# ignore/discard this line
			sys.stderr.write(f'Illegel value: {rating}')
			continue

		mean_rating += rating
		#print('%s\t%s' % (movie + ':' + movie, str(rating) + ':' + str(rating)))

	mean_rating /= len(movie_ratings)

	for itemRating1, itemRating2 in combinations(movie_ratings, 2):
		itemRating1 = itemRating1.split(':')
		itemRating2 = itemRating2.split(':')

		movieID1 = itemRating1[0]
		rating1 = itemRating1[1]
		movieID2 = itemRating2[0]
		rating2 = itemRating2[1]

		# Produce both orders so sims are bi-directional
		print('%s\t%s\t%s\t%s\t%s' % (movieID1, movieID2, rating1, rating2, mean_rating))
		print('%s\t%s\t%s\t%s\t%s' % (movieID2, movieID1, rating2, rating1, mean_rating))
