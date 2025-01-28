#!/usr/bin/env python3
import sys
from collections import defaultdict

# 단어 카운트 저장
word_counts = defaultdict(int)

# 표준 입력으로부터 데이터 읽기
for line in sys.stdin:
    try:
        # 단어와 숫자 분리
        word, count = line.strip().split('\t', 1)
        word_counts[word] += int(count)
    except:
        continue

# 결과 출력
for word, count in word_counts.items():
    print(f"{word}\t{count}")
