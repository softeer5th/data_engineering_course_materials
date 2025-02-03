#!/usr/bin/env python3
import sys

for line in sys.stdin:
    _, movie_id, rating, _ = line.strip().split(',')
    print(f'{movie_id}\t{rating}')