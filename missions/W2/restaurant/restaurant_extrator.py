import requests
import os
import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup

# returns a list of restaurants located within a r from the coordinates x and y
# parameters: x, y, r, Kakao developer API key
# return: json file of response

def get_restaurants(x, y, radius, api_key):
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
        raise Exception(f'Response status code: {response.text}')
    
# return ratings and reivew of the restaurant
# parameter: url of restaurant
# return json of restaurant

def get_review_html(url):
    # 셀레니움 드라이버 설정 (웹드라이버 매니저를 사용하여 크롬 드라이버 자동 관리)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(url)

    # 페이지 로딩 대기
    time.sleep(1.0)

    # 총 리뷰 수 확인
    soup = BeautifulSoup(driver.page_source, "html.parser")
    num_reviews_soup = soup.select_one('#mArticle > div.cont_evaluation > strong.total_evaluation > span')
    if num_reviews_soup is None:
        html = ' '
        driver.close()
        return html
    
    num_reviews = int(num_reviews_soup.text)

    pages = num_reviews//5  # 5개 리뷰 당 한 페이지
    # 스크롤을 통해 모든 리뷰 가져오기
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.2)
        # 스크롤 후 높이 체크
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # 리뷰 더보기 클릭
    for i in range(pages):
        next_page = driver.find_element(By.CSS_SELECTOR, '.evaluation_review .link_more')
        next_page.send_keys(Keys.ENTER)
        time.sleep(0.1)

    html = driver.page_source
    # 드라이버 종료
    driver.close()
    return html

# saves the receive data to a json file
# data: data to save, path: path to save, filename: filename.
# return: Succes code or Error code

def save_data_to_file(data, path, filename, mode):
    # 디렉터리 확인 및 생성
    if not os.path.exists(path):
        os.makedirs(path)
    
    # 파일 저장
    file_path = os.path.join(path, filename)
    file_name, file_ext = os.path.splitext(filename)
    try:
        with open(file_path, mode) as file:
            if  file_ext == 'json':
                json.dump(data, file, ensure_ascii=False, indent=4)
            elif file_ext == 'html':
                file.write(data)
            else:
                raise Exception('Wrong file type')
    except Exception as e:
        return e
    
def extract(x, y, radius, api_key, path, restaurants_filename, reviews_filename):
    restaurants_json = get_restaurants(x, y, radius, api_key)
    save_data_to_file(restaurant_json, path, restaurants_filename, 'w')
    reviews_json = {}
    try:
        for restaurant_json in restaurants_json['documents']:
            restaurant_id = restaurant_json['id']
            url = restaurant_json['url']
            reviews_json[id] = get_review_data(url)
            save_data_to_file(reviews_json, path, reviews_filename, 'a')
    except:
        pass