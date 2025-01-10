# 기본 설계
# ETL class와 E, T, L 메서드
# E, T, L을 수행하는 run 메서드
# run 메서드에서 E는 스레딩, T는 프로세싱으로 실행
# queue get, put을 이용하여 E가 완료되면 T가 겟으로 가져가게 함

# Extract 흐름
# 1. raw_data 만들기
# 1-1. url로부터 리스폰스 받아옴
# 1-2. 추출한 시간 now
# 1-3. json(url, text, date) 만들기
# 2. raw_data 저장 ()

# Transform 흐름
# 1. 테이블 찾기
# 2. 테이블 to DF
# 3. 원하는 데이터 뽑기

import pandas as pd
from bs4 import BeautifulSoup
import extract as e
from io import StringIO
import sqlite3
from pprint import pprint


# 1
class ETL:
    def Extract(self, url, path_raw_data):
        # url에서 raw data 가져오기
        js = e.get_raw_data(url)

        # raw data 저장
        e.save_raw_data(js, path_raw_data)

        return js

    def Transform(self, raw_data, path_region, css_selector):
        # 1. 테이블 찾기
        soup = BeautifulSoup(raw_data["text"], "lxml")  # html 파싱
        table = soup.select_one(css_selector)  # fmt:skip

        # 2. Table to DF
        df = pd.read_html(StringIO(str(table)))[0]

        # 3. formatting
        # 3-1. 멀티인덱스 제거
        df = df.droplevel(0, axis=1)

        # 4. filtering
        # 4-1. 원하는 열 선택
        df = df[["Country/Territory", "Forecast"]]
        # 4-2. 열 이름 변경
        df.columns = ["Country", "GDP_USD_billion"]
        # 4-3. 값이 이상한 행 제거(NaN)
        df = df[df["GDP_USD_billion"].str.isdigit()]
        # 4-4  astype
        df = df.astype({"GDP_USD_billion": "float"})
        # 4-5 지역 추가
        with open(path_region, "r") as f:
            region_df = pd.read_csv(f)
        region_df = region_df[["name", "region"]]  # 국가명, 지역 선택
        region_df.columns = ["Country", "Region"]  # 열 이름 변경
        df = pd.merge(
            df, region_df, how="left", on="Country"
        )  # Country 이름 기준으로 merge
        # 4-5. 국가가 아닌 행 제거(국가-지역 대조하면서 국가가 아닌 행은 지역에 NaN이 붙음)
        df = df.dropna()
        # 4-6. 단위 1B로 변환
        df["GDP_USD_billion"] = (df["GDP_USD_billion"] / 1000).round(2)

        return df

    def Load(self, df, path_db, table_name):
        # DB 연결
        con = sqlite3.connect(path_db)
        cursor = con.cursor()
        df.to_sql(name=table_name, con=con, if_exists="replace")
        con.close()

    def run(self, url, path_raw_data, path_region, path_db, css_selector, table_name):
        raw_data = self.Extract(url, path_raw_data)
        df = self.Transform(raw_data, path_region, css_selector)
        self.Load(df, path_db, table_name)

        return df


# Const block
URL = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
PATH_RAW_DATA = "missions/W1/M3/temp/data/Countries_by_GDP.json"
PATH_REGION = "missions/W1/M3/temp/data/region.csv"
PATH_DB = "missions/W1/M3/temp/data/World_Economies.db"
CSS_SELECTOR = ".wikitable.sortable.sticky-header-multi.static-row-numbers"
TABLE_NAME = "Countries_by_GDP"

# main

process = ETL()
df = process.run(URL, PATH_RAW_DATA, PATH_REGION, PATH_DB, CSS_SELECTOR, TABLE_NAME)

# 주어진 문제 해결하기 without sql
# 1. GDP가 100B 이상인 국가
print("GDP over 100B")
print(df[["Country", "GDP_USD_billion"]][df["GDP_USD_billion"] > 100])

# 2. 지역별 GDP 탑 5의 평균
print("GDP avg of Top 5 by region")
regions = set(df["Region"])
for region in regions:
    top = df[df["Region"] == region]["GDP_USD_billion"].head()
    print(region, top.mean())


# 주어진 문제 해결하기 with sql
def execute_sql(cursor, sql):
    cursor.execute(sql)
    rows = cursor.fetchall()

    return rows


# DB 연결
con = sqlite3.connect(PATH_DB)
cursor = con.cursor()

# 1. GDP가 100B 이상인 국가
print("GDP over 100B")
print("Country             |    GDP")
sql = """
    SELECT Country, GDP_USD_billion
    FROM Countries_by_GDP
    WHERE GDP_USD_billion > 100
    """

rows = execute_sql(cursor, sql)
for row in rows:
    print("%-20s" % row[0], row[1])

# 2. 지역별 GDP 탑 5의 평균
# 지역들 구하기
print("\nAVG GDP Of Top5 by region")
print("Region   | AVG GDP")
sql = """
    SELECT DISTINCT Region
    FROM Countries_by_GDP
"""
cursor.execute(sql)
regions = cursor.fetchall()

for region in regions:
    sql = (
        """
    SELECT AVG(GDP_USD_billion)
    FROM (SELECT GDP_USD_billion FROM Countries_by_GDP WHERE Region = '%s' ORDER BY GDP_USD_billion DESC LIMIT 5)
    """
        % region
    )
    print("%-10s" % region[0], execute_sql(cursor, sql)[0][0])
