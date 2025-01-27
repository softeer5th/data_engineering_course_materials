import sys
import json
import signal

# BrokenPipeError 방지
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# Mapper 코드
for line in sys.stdin: 
    line = line.strip() 
    try:
        # JSON 객체 파싱 -> python 딕셔너리로 변환
        review = json.loads(line)

        # 필요한 필드(상품ID, 평점) 추출
        product_id = review.get('asin', None)
        rating = review.get('rating', None)

        if product_id and rating:
            # 상품 ID와 평점 출력
            print(f"{product_id}\t{rating}")
    except json.JSONDecodeError:
        # JSON 파싱 실패 시 건너뜀
        continue
