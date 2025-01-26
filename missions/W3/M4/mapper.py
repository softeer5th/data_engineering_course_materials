#!/usr/bin/env python3
import sys 
import io
sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding = 'latin-1')

def mapper():
    for data in sys.stdin:
        data = data.strip().split(",")
        sentiment = int(data[0].strip('""'))
        if sentiment == 0:
            print("negative\t1")
        elif sentiment == 2:
            print("neutral\t1")
        elif sentiment == 4:
            print("positive\t1")

mapper()


