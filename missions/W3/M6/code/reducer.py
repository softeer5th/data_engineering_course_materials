#!/usr/bin/env python
import sys

curr_id = None
curr_rating = 0
curr_cnt = 0

for line in sys.stdin:
    try:
        id, rating = line.strip().split('\t')
        rating = float(rating)
        
        if id == curr_id:
            curr_rating += rating
            curr_cnt += 1
        else:
            if curr_id:
                avg_rating = curr_rating / curr_cnt if curr_cnt > 0 else 0
                print(f"{curr_id}\t{avg_rating:.2f}")
            curr_id = id
            curr_rating = rating
            curr_cnt = 1
    except:
        continue

if curr_id:
    avg_rating = curr_rating / curr_cnt if curr_cnt > 0 else 0
    print(f"{curr_id}\t{avg_rating:.2f}")
