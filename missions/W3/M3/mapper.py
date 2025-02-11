#!/usr/bin/env python3
import sys 
import re 
#\t를 기준으로 키와 값 분리
def mapper():
    for line in sys.stdin:
        line = line.strip()
        words = re.findall(r'\w+', line.lower())
        for word in words:
            print("{word}\t1".format(word = word))

mapper()