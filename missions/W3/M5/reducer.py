import sys

current_movie = None #현재 영화의 아이디
current_sum = 0 #현재 영화의 누적 평점
current_count = 0

# Reducer code
for line in sys.stdin:
    line = line.strip()
    movie_id, rating = line.split('\t')
    rating = float(rating) #평점 형변환
    
    #현재 영화와 이전 영화가 같다면 개수 누적
    if current_movie == movie_id:
        current_sum += rating
        current_count += 1 
    #새로운 영화가 등장하면 이전 영화의 아이디와 평균 평점을 출력
    else:
        if current_movie:
            avg_rating = current_sum / current_count
            print(f"{current_movie}\t{avg_rating}") 
        #변수 초기화
        current_movie = movie_id
        current_sum = rating
        current_count = 1

# 마지막 영화 처리
if current_movie == movie_id:
    avg_rating = current_sum / current_count
    print(f"{current_movie}\t{avg_rating}") 