import os
import sys
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np

# Const block
PATH = 'missions/W2/restaurant/data/'
X = 127.06283102249932
Y = 37.514322572335935
R = 1000  # 반경
API_KEY = '15545f98091012bb544fdad53077cee2'
RAW_FILENAME = 'restaurant.json'

def get_restaurant(x, y, radius, api_key):
    """
    Kakao API를 사용하여 음식점 정보를 가져오는 함수.

    Args:
        x (float): 경도.
        y (float): 위도.
        radius (int): 검색 반경 (미터 단위).
        api_key (str): Kakao API 키.
    """
    group = 'FD6'  # 음식점 카테고리
    query = 'restaurant'  # 검색 키워드
    auth = 'KakaoAK ' + api_key
    url = 'https://dapi.kakao.com/v2/local/search/keyword.json?query={}&category_group_code={}&y={}&x={}&radius={}'.format(
        query, group, y, x, radius)
    headers = {'Authorization': auth}

    # API 요청
    response = requests.get(url, headers=headers)

    if response.status_code == 200:  # 성공 시
        data = response.json()
        return data
    else:  # 실패 시
        print(f"Error {response.status_code}: {response.text}")
        sys.exit()


def save_to_file(data, path, filename):
    """
    데이터를 JSON 파일로 저장하는 함수.

    Args:
        data (dict): 저장할 데이터.
        path (str): 저장할 폴더 경로.
        filename (str): 저장할 파일 이름.
    """
    # 디렉터리 확인 및 생성
    if not os.path.exists(path):
        os.makedirs(path)

    # 파일 저장
    file_path = os.path.join(path, filename)
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"JSON data saved to {file_path}")


def get_rating_from_url(url):
    # 웹 페이지 가져오기
    response = requests.get(url)
    
    if response.status_code != 200:
        return "페이지를 가져올 수 없습니다."

    # HTML 파싱
    soup = BeautifulSoup(response.text, 'html.parser')
    print(response.text)
    # 별점 정보 추출
    rating_tag = soup.find('span', class_='color_b')
    print(rating_tag)
    if rating_tag:
        # '점'을 제외한 숫자 부분만 추출
        rating = rating_tag.text.strip()
        return rating
    else:
        return np.NaN


# Extaract
# raw = get_restaurant(X, Y, R, API_KEY)
# save_to_file(raw, PATH, RAW_FILENAME)

# Transform
with open(PATH + 'restaurant.json') as f:
    raw = json.load(f)
df = pd.DataFrame(raw['documents'])
df = df[['id', 'place_url']]
print(df)

ratings = []
for place_url in df['place_url']:
    rating = get_rating_from_url(place_url)
    ratings.append(rating)
    break

print(ratings)