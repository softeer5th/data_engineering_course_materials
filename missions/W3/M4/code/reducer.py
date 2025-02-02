#!/usr/bin/env python
import sys

curr_target = None
curr_cnt = 0

for line in sys.stdin:
    try:
        target, cnt = line.strip().split('\t')
        cnt = int(cnt)
        
        if target == curr_target:
            curr_cnt += cnt
        else:
            if curr_target:
                print(f'{curr_target}\t{curr_cnt}')
            curr_target = target
            curr_cnt = cnt
    except:
        continue

if curr_target:
    print(f'{curr_target}\t{curr_cnt}')
