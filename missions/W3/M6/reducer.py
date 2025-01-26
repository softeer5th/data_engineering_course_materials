import sys

current_product = None  # 현재 상품 ID
current_sum = 0         # 평점 합계
current_count = 0       # 리뷰 개수

# Reducer 코드
for line in sys.stdin:
    line = line.strip()
    try:
        product_id, rating = line.split('\t')
        rating = float(rating)
    except ValueError:
        # 잘못된 데이터 건너뜀
        continue

    # 동일한 product_id에 대해 평점 누적
    if current_product == product_id:
        current_sum += rating
        current_count += 1
    else:
        # 이전 상품 출력
        if current_product:
            avg_rating = current_sum / current_count
            print(f"{current_product}\t{current_count}\t{avg_rating}")
        
        # 새로운 상품으로 초기화
        current_product = product_id
        current_sum = rating
        current_count = 1

# 마지막 상품 처리
if current_product:
    avg_rating = current_sum / current_count
    print(f"{current_product}\t{current_count}\t{avg_rating:.2f}")
