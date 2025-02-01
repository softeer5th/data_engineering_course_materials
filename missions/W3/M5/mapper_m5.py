#!/usr/bin/env python3

import sys
import csv

csv.field_size_limit(sys.maxsize)
reader = csv.reader(sys.stdin)

for row in reader :
    
    movie_id = row[1].strip()
    rating = row[2].strip()

    print(f"{movie_id}\t{rating}\t1")