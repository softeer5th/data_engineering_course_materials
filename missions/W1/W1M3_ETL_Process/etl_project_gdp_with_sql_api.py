import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import sqlite3
from tabulate import tabulate
from datetime import datetime
import os

# 상수 설정 (URL, 파일 경로 등)
TARGET_URL = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)'
YEAR = 2025
API_URL = f"https://www.imf.org/external/datamapper/api/v1/NGDPD?periods={YEAR}"
COUNTRIES_API_URL = "https://www.imf.org/external/datamapper/api/v1/countries"
REGION_MAPPING_FILE = 'data/all.json'
RAW_DATA_FILE = 'results/Countries_by_GDP_api.json'
PROCESSED_DATA_FILE = 'results/Countries_by_GDP_api_processed.json'
DB_FILE = 'sqliteDB/World_Economies.db' # SQLite DB 파일 경로
LOG_FILE = 'log/etl_project_with_sql_api_log.txt'

# 로그 기록 함수
def log_message(message):
    """
    etl_project_log.txt 파일에 로그 메시지를 기록하는 함수.
    실행 시간을 포함하여 "time, log" 형식으로 기록.
    """
    with open(LOG_FILE, 'a') as f:
        current_time = datetime.now().strftime('%Y-%B-%d-%H-%M-%S')
        f.write(f"{current_time}, {message}\n")

# 실행 구분선 추가 함수
def log_separator():
    """
    로그 파일에 실행 구분선을 추가하는 함수.
    새로운 실행이 시작될 때마다 실행 시간을 명확하게 구분해줌.
    """
    with open(LOG_FILE, 'a') as f:
        f.write("\n" + "="*50 + "\n")
        current_time = datetime.now().strftime('%Y-%B-%d-%H-%M-%S')
        f.write(f"🚀 New Execution at {current_time}\n")
        f.write("="*50 + "\n\n")

# 로그 데코레이터 (함수 시작/종료 시 자동으로 로그 기록)
def log_decorator(func):
    """
    함수의 실행 시작과 완료를 자동으로 로그에 기록하는 데코레이터.
    각 함수가 시작될 때 '시작', 완료될 때 '완료'로 표시하며, 결과가 숫자인 경우 결과 값도 기록.
    """
    def wrapper(*args, **kwargs):
        log_message(f"{func.__name__} 시작")
        result = func(*args, **kwargs)
        log_message(f"{func.__name__} 완료: {result if isinstance(result, int) else 'Success'}")
        return result
    return wrapper

# GDP ETL (추출, 변환, 적재) 클래스
class GDP_ETL:
    """
    GDP 데이터를 추출, 변환, 적재하는 ETL 파이프라인 클래스.
    ETL 프로세스의 각 단계를 함수로 나누어 관리하며, 웹에서 데이터를 가져와 가공 후 저장.
    """

    def __init__(self, url):
        self.url = url
        # self.soup = self._fetch_data()  # 웹 페이지에서 HTML 데이터 파싱

    # def _fetch_data(self):
    #     """
    #     웹 페이지에서 HTML을 요청하여 BeautifulSoup 객체로 반환.
    #     요청 상태에 따라 성공 또는 실패 메시지를 로그에 기록.
    #     """
    #     response = requests.get(self.url)
    #     if response.status_code == 200:
    #         log_message("웹 페이지 요청 성공")
    #     else:
    #         log_message(f"웹 페이지 요청 실패 - 상태 코드 {response.status_code}")
    #     return BeautifulSoup(response.content, 'html.parser')
    
    # DB 연결 및 쿼리 실행 (결과 반환 X) (삽입, 수정, 삭제 용도)
    def execute_query(self, query, params=None):
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            conn.commit()

    # DB 연결 및 쿼리 결과 조회 (결과 반환 O) (조회 용도)
    def fetch_query(self, query, params=None):
        with sqlite3.connect(DB_FILE) as conn:
            if params:
                return pd.read_sql(query, conn, params=params)
            return pd.read_sql(query, conn)

    @log_decorator
    def extract(self, year=2025):
        """
        API를 통해 2025년 GDP 데이터를 추출하고 데이터프레임으로 변환.
        - IMF API를 사용하여 2025년 GDP 데이터를 수집.
        - JSON 형식의 응답 데이터를 DataFrame으로 변환.
        - 추출한 데이터를 JSON 파일로 저장.
        """       
        try:
            # API 요청
            response = requests.get(API_URL)
            response.raise_for_status()
            log_message("API 요청 성공")
            data = response.json()    
        except requests.exceptions.RequestException as e:
            log_message(f"API 요청 실패: {e}")
            raise ValueError("API에서 데이터를 가져올 수 없습니다.") from e

        # JSON 데이터를 국가 코드와 GDP 값을 추출해 pandas DataFrame으로 변환
        try:
            values = data.get("values", {}).get("NGDPD", {})
            records = [
                {
                    'Country': country_code, # 국가 이름
                    'GDP_USD_billion': gdp_data.get(str(YEAR), None), # GDP 값
                }
                for country_code, gdp_data in values.items()
            ]
            df = pd.DataFrame(records)
            row_count = len(df)
            log_message(f"총 {row_count}개 행 추출됨")  # 총 추출된 데이터 수 로그
        except Exception as e:
            log_message(f"데이터 파싱 실패: {e}")
            raise ValueError("응답 데이터를 처리할 수 없습니다.") from e

        # Raw 데이터를 JSON 파일로 저장
        df.to_json(RAW_DATA_FILE, orient='records', indent=4)
        file_size = os.path.getsize(RAW_DATA_FILE) / 1024  # KB 단위로 파일 크기 계산
        log_message(f"파일 저장 완료: {RAW_DATA_FILE} ({file_size:.2f} KB)")
        return df

    @log_decorator
    def transform(self, df):
        """
        데이터프레임을 변환하여 필요한 데이터만 유지하고 GDP 데이터를 가공.
        - GDP 데이터를 소숫점 둘째 자리까지 반올림.
        - 국가 코드와 지역(region)을 매핑.
        - 국가 코드와 국가 이름을 매핑.
        - 처리된 데이터를 JSON 파일로 저장.
        """

        df['GDP_USD_billion'] = df['GDP_USD_billion'].round(2)  # 소숫점 2자리까지 반올림

        try:
            # 국가 코드와 지역 매핑
            with open(REGION_MAPPING_FILE, 'r') as file:
                region_mapping_data = json.load(file)
        except Exception as e:
            log_message(f"지역 매핑 데이터 로드 실패: {e}")
            raise ValueError("지역 매핑 데이터를 가져올 수 없습니다.") from e
        
        # 국가 코드(alpha-3) -> 지역(region) 매핑 딕셔너리 생성
        region_mapping = {entry['alpha-3']: entry['region'] for entry in region_mapping_data}

        # 데이터프레임에 region 추가
        df['Region'] = df['Country'].map(region_mapping)

        # 매핑되지 않은 항목 제거 (region 값이 없는 행)
        initial_count = len(df)
        df = df.dropna(subset=['Region']).copy()
        intermediate_count = len(df)
        removed_for_region = initial_count - intermediate_count
        log_message(f"지역(region) 매핑 실패로 제거된 {removed_for_region}개 행")

        try:
            # 국가 이름과 국가 코드 매핑용 데이터 요청
            response = requests.get(COUNTRIES_API_URL)
            response.raise_for_status()
            log_message("국가 코드 데이터 요청 성공")
            countries_data = response.json()
        except requests.exceptions.RequestException as e:
            log_message(f"국가 코드 데이터 요청 실패: {e}")
            raise ValueError("국가 코드 데이터를 가져올 수 없습니다.") from e
        
        # 국가 코드 -> 국가 이름 매핑 딕셔너리 생성
        country_mapping = {code: info['label'] for code, info in countries_data.get('countries', {}).items()}

         # 매핑: 국가 코드 -> 국가 이름
        df['Country'] = df['Country'].map(country_mapping)

        # 국가 이름이 없는 행 제거 (매핑 실패)
        initial_count = len(df)
        df = df.dropna(subset=['Country']).copy()
        final_count = len(df)
        removed_count = initial_count - final_count
        log_message(f"매핑되지 않은 {removed_count}개 행 제거됨")

        # 처리된 데이터 저장
        df.to_json(PROCESSED_DATA_FILE, orient='records', indent=4)
        file_size = os.path.getsize(PROCESSED_DATA_FILE) / 1024  # KB 단위로 파일 크기 계산
        log_message(f"파일 저장 완료: {PROCESSED_DATA_FILE} ({file_size:.2f} KB)")

        return df

    @log_decorator
    def load(self, df):
        """
        변환된 데이터를 sqlite3 DB에 저장.
        """
        # SQLite 데이터베이스 연결
        with sqlite3.connect(DB_FILE) as conn:
            df.to_sql('Countries_by_GDP', conn, if_exists='replace', index=False)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM Countries_by_GDP")
            row_count = cur.fetchone()[0]
        log_message(f"데이터베이스 저장 완료: {row_count}개 행 저장됨")
        return row_count

    @log_decorator
    def print_gdp_over_100b(self):
        """
        GDP가 100B USD 이상인 국가를 출력.
        """
        query = """
        SELECT Country, GDP_USD_billion
        FROM Countries_by_GDP
        WHERE GDP_USD_billion >= 100
        ORDER BY GDP_USD_billion DESC;
        """
        df = self.fetch_query(query)

        df.index = range(1, len(df) + 1)
        print("\n🌍 GDP 100B USD 이상 국가 목록:")
        print(tabulate(df, headers='keys', tablefmt='grid'))
        log_message(f"GDP 100B 이상 국가 {len(df)}개 출력됨")
        return len(df)

    @log_decorator
    def print_region_avg_gdp(self):
        """
        지역별 상위 5개 국가의 GDP 평균을 계산하여 출력.
        """
        query = """
        SELECT Region, ROUND(AVG(GDP_USD_billion),2) AS "Top 5 Avg GDP"
        FROM (
            SELECT Country, Region, GDP_USD_billion,
                   ROW_NUMBER() OVER (PARTITION BY Region ORDER BY GDP_USD_billion DESC) AS rank
            FROM Countries_by_GDP
        )
        WHERE rank <= 5
        GROUP BY Region
        ORDER BY "Top 5 Avg GDP" DESC;
        """
        df = self.fetch_query(query)

        df.index = range(1, len(df) + 1)
        print("\n📊 Region별 상위 5개 국가의 GDP 평균:")
        print(tabulate(df, headers='keys', tablefmt='grid'))
        log_message(f"Region별 GDP 평균 계산 완료 (총 {len(df)}개 지역)")
        return len(df)


def main():
    log_separator()
    etl = GDP_ETL(API_URL)
    raw_df = etl.extract()
    transformed_df = etl.transform(raw_df)
    etl.load(transformed_df)
    etl.print_gdp_over_100b()
    etl.print_region_avg_gdp()


if __name__ == '__main__':
    main()