#!/usr/bin/env python3

import sys

current_movie_id = None
current_rating = 0
current_count = 0

for line in sys.stdin :
    
    try :
        movie_id, rating, count = line.strip().split('\t')
        rating = float(rating)
        count = int(count)
    except ValueError :
        continue

    if current_movie_id == movie_id :
        current_rating += rating
        current_count += count
    else :
        if current_movie_id :
            print(f"{current_movie_id}\t{current_rating/current_count:.2f}")
        current_movie_id = movie_id
        current_rating = rating
        current_count = count

if current_movie_id :
    print(f"{current_movie_id}\t{current_rating/current_count:.2f}")
