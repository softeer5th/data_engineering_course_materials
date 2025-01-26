#!/usr/bin/env python3
import sys 

def reducer():
    current_movie = None 
    total = 0 
    count = 0

    for line in sys.stdin:
        movieId, rating = line.split('\t')
        if movieId != current_movie:
            if current_movie:
                print('{current_movie}\t{total:.1f}'.format(current_movie=current_movie, total = total / count))
            current_movie = movieId 
            count = 0
            total = 0
        count += 1
        total += float(rating)
    print('{current_movie}\t{total:.1f}'.format(current_movie=current_movie, total = total / count))
reducer()