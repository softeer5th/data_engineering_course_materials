#!/usr/bin/env python
import sys
import string

PUNCTUATION = string.punctuation

for line in sys.stdin:
    words = line.strip().split()
    for word in words:
        word = word.strip(PUNCTUATION)
        print(f'{word}\t1')