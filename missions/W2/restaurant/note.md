# 카카오맵 평점 + 데이터 수집
1. 평점이 낮은 가게들의 리뷰 특징 + 높은 가게들의 리뷰 특징.

2. 사람이 기준보다 평점을 높게 줬을 때 자주 사용하는 단어 vs 낮게 줬을 때 자주 사용하는 단어

# Extrator 모듈 설계

가게ID, 가게 평균 평점

가게ID, 리뷰 평점, 준 손님의 평균 평점, 리뷰 텍스트


1. 가게 리스트 api로 반환받자

2. 가게 리스트 url마다 방문해서 평점, 손님 평균 평점, 리뷰 텍스트 수집

그럼 json 구조를

[
    {
        "restaurant": "밥집",
        "reviews": [
            {
                "score": 4.3,
                "avg_score": 3.9,
                "text": "밥이 맛있어요"
            },
        ]
    },
    {
        "restaurant": "파스타집",
        "reviews": [
            {
                "score": 3.2,
                "avg_score": 3.9,
                "text": "파스타가 짜요"
            },
            {
                "score": 4.5,
                "avg_score": 4.0,
                "text": "분위기가 좋아요"
            }
        ]
    },
]

뭐 이런 식으로 해서 추출하자. 

----- 아니다 걍 셀레니움

'''
{
    'restaurant_id':1234,
    'reviews':[
        {
        'rating': 3
        'customer_avg_rating': 4
        'text' : '맛이 없다'
        }
    ]
}
'''