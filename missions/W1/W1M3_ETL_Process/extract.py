import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from utils import log_message

TARGET_URL = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)'
RAW_DATA_FILE = 'results/Countries_by_GDP.json'

def extract():
    response = requests.get(TARGET_URL)
    if response.status_code == 200:
        log_message("웹 페이지 요청 성공")
        soup = BeautifulSoup(response.content, 'html.parser')
    else:
        log_message(f"웹 페이지 요청 실패 - 상태 코드 {response.status_code}")
        raise ValueError("데이터 추출 실패")

    table = soup.select_one('table.wikitable')
    if not table:
        log_message("⚠️ GDP 테이블을 찾을 수 없습니다.")
        raise ValueError("데이터 테이블이 존재하지 않습니다.")
    
    rows = table.select('tr')

    # 컬럼 매핑을 위한 설정 (유지보수 편의성)
    col_mapping = {
        'Country': 0,
        'GDP_USD_billion': 1,
        'Year': 2
    }

    # 테이블의 각 행에서 국가, GDP, 연도 정보 추출
    df = pd.DataFrame([
        {
            'Country': cols[col_mapping['Country']].get_text(strip=True),
            'GDP_USD_billion': cols[col_mapping['GDP_USD_billion']].get_text(strip=True),
            'Year': re.sub(r'\[.*?\]', '', cols[col_mapping['Year']].get_text(strip=True)) # 주석 제거
        }
        for row in rows
        if len(cols := row.find_all('td')) >= 3
    ])
    
    df.to_json(RAW_DATA_FILE, orient='records', indent=4)
    log_message(f"{len(df)}개 행 추출 및 저장 완료: {RAW_DATA_FILE}")

if __name__ == '__main__':
    extract()