#!/usr/bin/env python3
import sys 
#\t를 기준으로 키와 값 분리
def mapper():
    for line in sys.stdin:
        line = line.strip()
        if line.startswith('userId,movieId,rating,timestamp'):
            continue

        fields = line.split(",")
        print("{movieId}\t{rating}".format(movieId = fields[1], rating=fields[2]))

mapper()