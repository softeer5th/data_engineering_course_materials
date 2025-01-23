import sys

# Mapper code
for line in sys.stdin:
    try:
        line = line.strip()  # 공백 제거
        words = line.split()  # 단어 분리
        for word in words:
            print(f"{word}\t1")  # 각 단어를 '단어\t1' 형식으로 출력
    except:
        continue
