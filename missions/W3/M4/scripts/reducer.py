#!/usr/bin/python3
import sys
from collections import defaultdict

sentiment_count = defaultdict(int)

for line in sys.stdin:
    line = line.strip()
    try:
        sentiment, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue
    sentiment_count[sentiment] += count

for sentiment, count in sentiment_count.items():
    print(f"{sentiment}\t{count}")
