#!/usr/bin/env python
import sys

curr_id = None
curr_rating = 0
curr_cnt = 0

for line in sys.stdin:
    try:
        id, rating, cnt = line.strip().split('\t')
        rating = float(rating)
        cnt = int(cnt)
        
        if id == curr_id:
            curr_rating += rating
            curr_cnt += cnt
        else:
            if curr_id:
                avg_rating = curr_rating / curr_cnt if curr_cnt > 0 else 0
                print(f"{curr_id}\t{curr_rating}\t{curr_cnt}\t{avg_rating:.2f}")
            curr_id = id
            curr_rating = rating
            curr_cnt = cnt
    except:
        continue

if curr_id:
    avg_rating = curr_rating / curr_cnt if curr_cnt > 0 else 0
    print(f"{curr_id}\t{curr_rating}\t{curr_cnt}\t{avg_rating:.2f}")
