#!/usr/bin/env python3

import sys

# Mapper: 한 줄씩 입력 받아 단어를 키로, 1을 값으로 출력
for line in sys.stdin:
    # 줄바꿈 제거 및 단어별로 분리
    words = line.strip().split()
    for word in words:
        # 각 단어를 출력 (Hadoop에서는 자동으로 키-값 쌍으로 정렬)
        print(f"{word}\t1")
