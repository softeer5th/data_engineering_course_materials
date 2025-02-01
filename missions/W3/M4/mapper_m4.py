#!/usr/bin/env python3

import sys
import csv

csv.field_size_limit(sys.maxsize)
reader = csv.reader(sys.stdin)

sentiment_dict = {
    '0' : 'negative',
    '2' : 'neutral',
    '4' : 'positive'
}

for row in reader :

    sentiment_num = row[0].strip()

    sentiment = sentiment_dict.get(sentiment_num)

    print(f"{sentiment}\t1")

