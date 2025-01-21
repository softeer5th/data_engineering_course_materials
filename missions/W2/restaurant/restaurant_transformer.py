import pandas as pd
from bs4 import BeautifulSoup

# 필요한 거
# 가게 아이디 + 리뷰

# parameter:

def get_reviews_from_json(reviews_json):
    ids_texts = reviews_json['restaurants'] # [{id:~ restaurant:~}, ~~ 반복인 리스트]
    ids_reviews = []

    for id_text in ids_texts:  # 리스트 내의 딕셔너리
        id_rating_reviews = get_id_reviews_from_id_text(id_text)
        if id_rating_reviews is not None:
            ids_reviews.append(id_rating_reviews)

    return ids_reviews

def get_id_reviews_from_id_text(id_text):
    id = id_text['id']
    text = id_text['text']

    reviews = []
    soup = BeautifulSoup(text, 'lxml')
    comments = soup.find_all('p', class_= 'txt_comment')
    for comment in comments:
        span = comment.find('span').text 
        if span:
            reviews.append(span)

    rating_tag = soup.find(class_='color_b')
    if rating_tag is None:
        return None
    rating = rating_tag.text

    return {'id': id, 'rating': rating, 'reviews': reviews}

def get_df_from_ids_reviews(ids_reviews):
    return pd.DataFrame(ids_reviews)