# 기본 설계
# ETL class와 E, T, L 메서드
# E, T, L을 수행하는 run 메서드
# run 메서드에서 E는 스레딩, T는 프로세싱으로 실행
# queue get, put을 이용하여 E가 완료되면 T가 겟으로 가져가게 함

# Extract 흐름
# 1. url로부터 리스폰스 받아옴
# 2. raw_data 저장 ()

# Transform 흐름
# 1. 테이블 찾기
# 2. 테이블 to DF
# 3. 원하는 데이터 뽑기

import pandas as pd
from bs4 import BeautifulSoup
import requests


# 1
class ETL:
    def Extract(self, url, path_raw_data):
        # http 요청하여 리스폰스 받아오기
        response = requests.get(url)

        # raw data 저장
        with open(path_raw_data, "w") as f:
            f.write(response.text)

        return response.text
        print("E")

    def Transform(self, raw_data, css_selector):
        # 테이블 찾기
        soup = BeautifulSoup(raw_data, "lxml")
        table = soup.select_one(css_selector)

        # Table to DF
        df = pd.read_json(table)
        print(table)
        print("T")

    def Load(self):
        print("L")

    def run(self, url):
        raw_data = self.Extract(url)
        self.Transform(raw_data, css_selector)
        self.Load()


# Const block
URL = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
CSS_SELECTOR = "wikitable sortable sticky-header-multi static-row-numbers"
PATH_RAW_DATA = "missions/W1/M3/temp/data/Countries_by_GDP.json"
PATH_REGION = "missions/W1/M3/temp/data/region.csv"
PATH_LOG = "missions/W1/M3/temp/data/etl_project_log.txt"

# main

process = ETL()
process.run(URL, CSS_SELECTOR)
