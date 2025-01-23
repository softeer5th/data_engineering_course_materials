#!/usr/bin/env python
import sys
import csv

lines = csv.reader(sys.stdin)
for line in lines:
    user_id, movie_id, rating, timestamp = line
    try:
        user_id = int(user_id)
    except ValueError:
        continue
    else:
        print(f'{movie_id}\t{rating}')
