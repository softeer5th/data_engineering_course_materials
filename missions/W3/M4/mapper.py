import sys
import csv

for line in sys.stdin:
    line = line.strip()  # 공백 제거
    try:
        # CSV 형식으로 한 줄 데이터를 파싱
        row = list(csv.reader([line]))[0]  # 한 줄 CSV 파싱
        target = row[0]  # 첫 번째 컬럼: target
        sentiment = "neutral"  # 기본값: neutral

        # target 값에 따라 감정 분류
        if target == "0":
            sentiment = "negative"
        elif target == "4":
            sentiment = "positive"

        # 결과 출력
        print(f"{sentiment}\t1")
    except Exception:
        # CSV 파싱 실패 시 무시
        continue
