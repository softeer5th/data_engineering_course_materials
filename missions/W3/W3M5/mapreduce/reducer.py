#!/usr/bin/env python3
import sys

current_movie_id = None
total_rating = 0
total_rating_count = 0

for line in sys.stdin:
    movie_id, rating = line.strip().split('\t')

    try:
        rating = float(rating)
    except ValueError:
        continue

    if current_movie_id != movie_id:
        if current_movie_id:
            average_rating = total_rating / total_rating_count
            print(f'{current_movie_id}\t{average_rating:.1f}')
        current_movie_id = movie_id
        total_rating = rating
        total_rating_count = 1
    else:
        total_rating += rating
        total_rating_count += 1

if current_movie_id == movie_id:
    average_rating = total_rating / total_rating_count
    print(f'{current_movie_id}\t{average_rating:.1f}')