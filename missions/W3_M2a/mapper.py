#!/usr/bin/env python3
import sys

# 표준 입력으로부터 데이터 읽기
for line in sys.stdin:
    # 입력 라인을 공백으로 구분하여 단어 리스트로 만듦
    words = line.strip().split()
    for word in words:
        # 각 단어와 함께 숫자 1 출력
        print(f"{word}\t1")