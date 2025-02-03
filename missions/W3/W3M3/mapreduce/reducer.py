#!/usr/bin/env python3
import sys

# 단어와 빈도수를 저장할 딕셔너리
word_counts = {}

# 표준 입력에서 데이터 읽기
for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)

    try:
        count = int(count)
    except ValueError:
        continue  # 숫자로 변환할 수 없는 경우 무시

    # 단어 빈도수 누적
    if word in word_counts:
        word_counts[word] += count
    else:
        word_counts[word] = count

# 빈도수 기준으로 내림차순 정렬 후 출력
sorted_word_counts = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)

for word, count in sorted_word_counts:
    print(f'{word}\t{count}')