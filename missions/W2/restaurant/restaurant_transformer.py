import pandas as pd
from bs4 import BeautifulSoup

# 필요한 거
# 가게 아이디 + 리뷰

def get_reviews_from_json(reviews_json):
    ids_texts = reviews_json['restaurants'] # [{id:~ restaurant:~}, ~~ 반복인 리스트]
    ids_reviews = []

    for id_text in ids_texts:  # 리스트 내의 딕셔너리
        id = id_text['id']
        text = id_text['text']

        reviews = []
        soup = BeautifulSoup(text, 'lxml')
        comments = soup.find_all('p', class_= 'txt_comment')
        for comment in comments:
            span = comment.find('span').text 
            if span:
                reviews.append(span)
                
        # reviews에 comments text 다 들어가 있다.
        # {'id' = id, 'comments' = [comments]}
        ids_reviews.append({'id': id, 'reviews': reviews})