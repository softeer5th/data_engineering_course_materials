#!/usr/bin/env python3
import sys 

def reducer():
    current_sentiment = None 
    total = 0 

    for data in sys.stdin:
        sentiment, count = data.split('\t')
        if sentiment != current_sentiment:
            if current_sentiment:
                print('{current_sentiment}\t{total}'.format(current_sentiment=current_sentiment, total = total))
            current_sentiment = sentiment 
            total = 0
        total += int(count)
    print('{current_sentiment}\t{total}'.format(current_sentiment=current_sentiment, total = total))
    
reducer()

