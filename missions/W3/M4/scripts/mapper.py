#!/usr/bin/python3
import io
import sys
import csv


input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='latin-1')

def mapper():
    sentiment_map = {
        0: 'negative',
        2: 'neutral',
        4: 'positive'
    }
    
    reader = csv.reader(input_stream)
    for row in reader:
        if not row:
            continue
        
        try:
            sentiment_value = int(row[0])
        except ValueError:
            continue

        sentiment = sentiment_map.get(sentiment_value)
        if sentiment is None:
            continue

        print(f"{sentiment}\t1")

if __name__ == "__main__":
    mapper()
