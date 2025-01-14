# 표준 라이브러리
import os
import json
import time
from typing import Optional

# 서드파티 라이브러리
import requests
from bs4 import BeautifulSoup


# 로컬 모듈
from missions.W2.M5.log.log import Logger

logger = Logger.get_logger("BLIND_REVIEW_EXTRACT")

###########
#  1. Fetch
###########
def _fetch_web(url: str)-> Optional[str]:
    # 브라우저 정보를 흉내내는 User-Agent를 넣어주면 안정성을 높일 수 있다.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/109.0.0.0 Safari/537.36"
        )
    }

    try:
        logger.info(f'[FETCH] 요청 송신: {url}')
        start_time = time.time()

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f'[FETCH] 응답 수신: {url} (소요시간: {elapsed_time:.3f}초)')
        return response.text
    except (requests.exceptions.HTTPError, Exception) as e:
        return None
    
###########
#  2. Parse
###########
def _parse_review(review: any):
    # title
    rvtit_h3 = review.find('h3', class_="rvtit")
    title = rvtit_h3.get_text(strip=True)[1:-1]

    # rating
    rating_div = review.find('div', class_="rating")
    if rating_div:
        strong_tag = rating_div.find("strong", class_="num")
    if strong_tag:
        i_tag = strong_tag.find("i", class_="blind")
        if i_tag:
            i_tag.decompose()

        rating = strong_tag.get_text(strip=True)

    # status & info
    status = '전직원'
    auth_div = review.find('div', class_="auth")
    strong_tag = auth_div.find("strong", class_="vrf")
    if strong_tag:
        span_tag = strong_tag.find("span", class_="ico_vrf")
        if span_tag:
            span_tag.decompose()
        status = strong_tag.get_text(strip=True)
        strong_tag.decompose()
    info = auth_div.get_text(strip=True)

    return {'title': title, 'rating': rating, 'status': status, 'info': info}

def _parse_html(url: str, raw_html: str)-> Optional[dict]:
    logger.info(f'[PARSE] 웹페이지 파싱 시작: {url}')
    start_time = time.time()

    soup = BeautifulSoup(raw_html, "html.parser")
    reviews = soup.find_all(attrs={"class": "review_item_inr"})
    review_data = []
    for idx, review in enumerate(reviews):
        try:
            review_data.append(_parse_review(review))
        except:
            logger.info(f'[PARSE] 웹페이지 파싱 중 실패: idx: {review})')
            return None
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f'[PARSE] 웹페이지 파싱 완료: {url} (소요시간: {elapsed_time:.3f}초)')
    return review_data

###########
#  3. Save to JSON
###########
def _save2json_replace(data: dict, file: str)-> Optional[int]:
    try:
        with open(file, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)
        return len(data)
    except Exception as e:
        return None

def _save2json_append(data: dict, file: str)-> Optional[int]:
    try:
        if not os.path.exists(file):
            logger.info(f'[FILE] {file} 생성 작업 중 ...')
            with open(file, "w", encoding="utf-8") as json_file:
                json.dump([], json_file, ensure_ascii=False, indent=4)
            logger.info(f'[FILE] {file} 생성 완료')

        logger.info(f'[FILE] {file} 로드 작업 중 ...')
        with open(file, "r", encoding="utf-8") as json_file:
            load_data = json.load(json_file)
        logger.info(f'[FILE] {file} 로드 완료')
        
        load_data.extend(data)

        logger.info(f'[FILE] {file}에 쓰기 작업 중 ...')
        with open(file, "w", encoding="utf-8") as json_file:
            json.dump(load_data, json_file, indent=4, ensure_ascii=False)
        logger.info(f'[FILE] {file}에 쓰기 완료: {len(data)} lines.')
        return len(data)
    except Exception as e:
        logger.info(f'[FILE] {file} 작업 실패')
        return None
    
def main():
    base_url = 'https://www.teamblind.com/kr/company/{0}/reviews?page={1}'
    save_path = 'missions/W2/M5/data/blind_hyundai_motors.json'

    MAX_TOTAL_LINE = 30

    logger.info(f'[EXTRACT] 추출 시작: 목표 row 수: {MAX_TOTAL_LINE}')
    start_time = time.time()

    page_no = 1
    total_rows = 0
    while total_rows <= MAX_TOTAL_LINE:
        url = base_url.format('현대자동차', page_no)
        raw_html = _fetch_web(url)
        review_data = _parse_html(url, raw_html)
        rows = _save2json_append(review_data, save_path)

        if not rows:
            continue
        page_no += 1
        total_rows += rows
    

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f'[EXTRACT] 추출 완료: 실제 row 수: {total_rows} (소요시간: {elapsed_time:.3f}초)')

if __name__ == "__main__":
    main()