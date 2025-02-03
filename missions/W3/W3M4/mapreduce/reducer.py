#!/usr/bin/env python3
import sys

sentiment_counts = {}

# 입력 데이터를 처리하여 감성별 개수를 딕셔너리에 저장
for line in sys.stdin:
    line = line.strip()
    sentiment, count = line.split('\t', 1)

    try:
        count = int(count)
    except ValueError:
        continue

    if sentiment in sentiment_counts:
        sentiment_counts[sentiment] += count
    else:
        sentiment_counts[sentiment] = count

# 감성별 개수를 정렬 (count 값 기준 내림차순 정렬)
sorted_sentiments = sorted(sentiment_counts.items(), key=lambda x: x[1], reverse=True)

# 정렬된 결과 출력
for sentiment, count in sorted_sentiments:
    print(f'{sentiment}\t{count}')