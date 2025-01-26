#!/usr/bin/env python
import sys
import csv

lines = csv.reader(sys.stdin)
for line in lines:
    target, ids, date, flag, user, text = line
    if target == "0":
        print("negtative\t1")
    elif target == "4":
        print("positive\t1")
    else:
        print("neutral\t1")