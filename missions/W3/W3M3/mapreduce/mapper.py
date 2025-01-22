#!/usr/bin/env python3

import sys

# Reading input from standard input
for line in sys.stdin:
    line = line.strip()
    words = line.split()
    
    # Emit the word count pairs
    for word in words:
        print(f"{word}\t1")