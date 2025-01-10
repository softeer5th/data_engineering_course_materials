import pandas as pd
from bs4 import BeautifulSoup
import extract as e
from io import StringIO
import sqlite3
from multiprocessing import Process, Queue
import datetime as dt


# 1
class ETL:
    def Extract(self, url, path_raw_data, path_log):
        write_log("Extract start", path_log)
        # url에서 raw data 가져오기
        js = e.get_raw_data(url)

        # raw data 저장
        e.save_raw_data(js, path_raw_data)
        write_log("Extract finished", path_log)
        return js

    def Transform(self, raw_data, path_region, css_selector, path_log):
        write_log("Transform start", path_log)

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

        write_log("Transform finished", path_log)
        return df

    def Load(self, df, path_db, table_name, path_log):
        write_log("Load start", path_log)

        # DB 연결
        con = sqlite3.connect(path_db)
        cursor = con.cursor()
        df.to_sql(name=table_name, con=con, if_exists="replace")
        con.close()

        write_log("Load finished", path_log)

    def run(
        self,
        url,
        path_raw_data,
        path_region,
        path_log,
        css_selector,
        table_name,
        path_db,
    ):
        raw_data = self.Extract(url, path_raw_data, path_log)
        df = self.Transform(raw_data, path_region, css_selector, path_log)
        self.Load(df, path_db, table_name, path_log)
        return df


# write_log
def write_log(log, path_log):
    now = dt.datetime.now()
    log = now.strftime("%Y-%b-%d-%H-%M-%S, ") + log + "\n"
    with open(path_log, "a") as f:
        f.write(log)


# Const block
URL = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
PATH_RAW_DATA = "missions/W1/M3/data/Countries_by_GDP.json"
PATH_REGION = "missions/W1/M3/data/region.csv"
PATH_LOG = "missions/W1/M3/data/etl_project_log.txt"
PATH_DB = "missions/W1/M3/data/World_Economies.db"
CSS_SELECTOR = ".wikitable.sortable.sticky-header-multi.static-row-numbers"
TABLE_NAME = "Countries_by_GDP"

# main
process = ETL()
df = process.run(
    URL, PATH_RAW_DATA, PATH_REGION, PATH_LOG, CSS_SELECTOR, TABLE_NAME, PATH_DB
)

# 주어진 문제 해결하기 with sql

# DB 연결
con = sqlite3.connect(PATH_DB)
cursor = con.cursor()

# 1. GDP가 100B 이상인 국가
print("GDP over 100B")
print("Country             |     GDP")
sql = """
    SELECT Country, GDP_USD_billion
    FROM Countries_by_GDP
    WHERE GDP_USD_billion > 100
    """

cursor.execute(sql)
rows = cursor.fetchall()

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
    sql = """
    SELECT AVG(GDP_USD_billion)
    FROM (SELECT GDP_USD_billion FROM Countries_by_GDP WHERE Region = ? ORDER BY GDP_USD_billion DESC LIMIT 5)
    """
    cursor.execute(sql, (region[0],))
    row = cursor.fetchone()
    print("%-10s" % region[0], row[0])

con.close()
