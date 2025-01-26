import sys
import csv

is_header = True #헤더 확인 플래그

for line in sys.stdin:
    line = line.strip()  # 공백 제거
    try:
        # CSV 데이터 파싱
        row = list(csv.reader([line]))[0]

        # 첫 번째 줄(헤더) 건너뛰기
        if is_header:
            is_header = False
            continue

        movie_id = row[1]  # 두 번째 컬럼: movieId
        rating = row[2]    # 세 번째 컬럼: rating

        # movieId와 rating 출력
        print(f"{movie_id}\t{rating}")
    except Exception:
        continue
