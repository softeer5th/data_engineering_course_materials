#!/usr/bin/env python3

import sys

# Reading input from standard input
for line in sys.stdin:
    values = line.strip().split(",")

    if values[0] == '"0"':
        print('negative\t1')
    elif values[0] == '"4"':
        print('positive\t1')
    else:
        print('neutral\t1')
