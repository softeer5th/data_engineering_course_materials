#!/usr/bin/env python3

import sys

current_sentiment = None
current_count = 0

# Reducer: 키(단어)를 그룹화하고, 값(1)을 합산
for line in sys.stdin:
    sentiment, count = line.strip().split("\t")
    count = int(count)
    if sentiment == current_sentiment:
        # 같은 단어가 계속 입력될 경우 합산
        current_count += count
    else:
        # 다른 단어가 들어오면 현재 단어의 합산 결과 출력
        if current_sentiment:
            print(f"{current_sentiment}\t{current_count}")
        current_sentiment = sentiment
        current_count = count

# 마지막 단어 출력
if current_sentiment:
    print(f"{current_sentiment}\t{current_count}")