#!/usr/bin/env python
import sys

curr_word = None
curr_cnt = 0

for line in sys.stdin:
    try:
        word, cnt = line.strip().split('\t')
        cnt = int(cnt)
        
        if word == curr_word:
            curr_cnt += cnt
        else:
            if curr_word:
                print(f'{curr_word}\t{curr_cnt}')
            curr_word = word
            curr_cnt = cnt
    except:
        continue

if curr_word:
    print(f'{curr_word}\t{curr_cnt}')
