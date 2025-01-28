#!/usr/bin/env python3
import sys 

def reducer():
    current_word = None 
    total = 0 

    for line in sys.stdin:
        word, count = line.split('\t')
        if word != current_word:
            if current_word:
                print('{current_word}\t{total}'.format(current_word=current_word, total = total))
            current_word = word 
            total = 0
        total += int(count)

reducer()