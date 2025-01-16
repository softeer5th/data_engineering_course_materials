import requests
import sqlite3
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import json
import configparser
import os

# 로그 시작 함수
def log_started():
    with open('etl_project_log.txt', 'a') as log_file:
        log_file.write("\n" + "="*50 + "\n")
        timestamp = datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S')
        log_file.write(f"New Execution at {timestamp}\n")
        log_file.write("="*50 + "\n\n")

# 로그 기록 함수
def log_message(message, level="INFO"):
    timestamp = datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S')
    with open('etl_project_log.txt', 'a') as log_file:
        log_file.write(f"{timestamp} - {level} - {message}\n")


# 설정 파일 읽기
def load_config(config_path='config.ini'):
    if not os.path.exists(config_path):
        log_message(f"Configuration file '{config_path}' not found.", level="ERROR")
        raise FileNotFoundError(f"Configuration file '{config_path}' not found.")
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    if 'DEFAULT' not in config or 'URL' not in config['DEFAULT'] or 'TABLE_CLASS' not in config['DEFAULT']:
        log_message("Invalid or missing configuration values in 'config.ini'.", level="ERROR")
        raise ValueError("Invalid or missing configuration values in 'config.ini'.")
    
    return config['DEFAULT']['URL'], config['DEFAULT']['TABLE_CLASS']


# Extract
def extract_gdp_data(url, table_class):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': table_class})

        if table is None:
            log_message("No table found with the specified class.", level="ERROR")
            raise ValueError("Failed to locate the GDP table on the webpage.")

        # 위키피디아에서 제공하는 표를 Pandas로 읽기
        df = pd.read_html(str(table))[0]
        
        # 필요한 칼럼만 선택 및 이름 변경
        df = df.iloc[:, [0, 1, 2]]  # 첫 번째, 두 번째, 세 번째 칼럼만 선택
        df.columns = ['Country', 'GDP (Nominal)', 'Year']  # 칼럼 이름 설정
        # NaN 데이터 제거
        df = df.dropna(subset=['Country', 'GDP (Nominal)'])
        # GDP 값 정리 및 변환
        df['GDP (B USD)'] = (
            df['GDP (Nominal)']
            .str.replace(r'[^\d.]', '', regex=True)  # 숫자와 소수점 이외 제거
            .replace('', '0')  # 빈 문자열을 '0'으로 대체
            .astype(float)  # float으로 변환
            / 1e3  # 단위를 B USD로 변환
        )
        # 각주 제거
        df['Year'] = df['Year'].str.replace(r'\[.*?\]', '', regex=True)
        # 필요한 3개 칼럼만 유지
        df = df[['Country', 'GDP (B USD)', 'Year']]
        # GDP 내림차순 정렬
        df = df.sort_values(by='GDP (B USD)', ascending=False)
        # 소수점 2자리로 반올림
        df['GDP (B USD)'] = df['GDP (B USD)'].round(2)
        return df
    except Exception as e:
        log_message(f"Error during data extraction: {str(e)}", level="ERROR")
        raise


# Transform
def transform_gdp_data(df):
    log_message("Starting Data Transformation")
    try:
        df = df.sort_values(by='GDP (B USD)', ascending=False)
        with open('country_region_table.json', 'r', encoding='utf-8') as region_file:
            region_data = json.load(region_file)
        df['Region'] = df['Country'].map(region_data)
        log_message("Data Transformation Completed")
        return df
    except Exception as e:
        log_message(f"Error during data transformation: {str(e)}", level="ERROR")
        raise


# Load
# Save to SQLite Database
def load_gdp_data(df):
    log_message("Loading data into SQLite database")
    try:
        # SQLite 데이터베이스에 연결
        conn = sqlite3.connect('World_Economies.db')

        # 데이터를 'Countries_by_GDP' 테이블로 저장
        # 'Country', 'GDP_USD_billion', 'Year', 'Region' 칼럼 포함
        df[['Country', 'GDP (B USD)', 'Year', 'Region']].rename(
            columns={'GDP (B USD)': 'GDP_USD_billion'}
        ).to_sql(
            'Countries_by_GDP', conn, if_exists='replace', index=False
        )

        conn.close()
        log_message("Data successfully loaded into SQLite database")
    except Exception as e:
        log_message(f"Error while loading data into SQLite database: {str(e)}", level="ERROR")
        raise


# GDP 100B USD 이상 국가 출력
def display_countries_with_gdp_over_100():
    log_message("Displaying countries with GDP over 100B USD")
    try:
        conn = sqlite3.connect('World_Economies.db')
        query = "SELECT Country, GDP_USD_billion FROM Countries_by_GDP WHERE GDP_USD_billion >= 100"
        result = pd.read_sql_query(query, conn)
        conn.close()
        print("Countries with GDP >= 100B USD:")
        print(result)
    except Exception as e:
        log_message(f"Error querying database for countries with GDP >= 100B: {str(e)}", level="ERROR")
        raise


# Region별 상위 5개 국가의 GDP 평균 계산 및 출력
def display_region_top5_average_gdp():
    log_message("Calculating average GDP of top 5 countries by region")
    try:
        conn = sqlite3.connect('World_Economies.db')
        query = """
        WITH RankedCountries AS (
            SELECT Country, GDP_USD_billion, Region,
                   RANK() OVER (PARTITION BY Region ORDER BY GDP_USD_billion DESC) AS Rank
            FROM Countries_by_GDP
            WHERE Region IS NOT NULL  -- Region이 None인 데이터를 제외
        )
        SELECT Region, AVG(GDP_USD_billion) AS Avg_Top5_GDP
        FROM RankedCountries
        WHERE Rank <= 5
        GROUP BY Region
        """
        result = pd.read_sql_query(query, conn)
        conn.close()
        print("Average GDP of top 5 countries by region (excluding None):")
        print(result)
    except Exception as e:
        log_message(f"Error querying database for top 5 average GDP: {str(e)}", level="ERROR")
        raise


# Save
def save_gdp_data(df, output_csv_file='extracted_gdp_data.csv', output_json_file='extracted_gdp_data.json'):
    log_message("Saving Extracted Data")
    try:
        df.to_csv(output_csv_file, index=False)
        df.to_json(output_json_file, orient='records', force_ascii=False, indent=4)
        log_message(f"Data saved: CSV ({output_csv_file}), JSON ({output_json_file})")
    except Exception as e:
        log_message(f"Failed to save data: {str(e)}", level="ERROR")
        raise


# GDP가 100B USD 이상인 국가 필터링
def filtered_100USD(df):
    filtered_100 = df[df['GDP (B USD)'] >= 100]
    print("Countries with a GDP of over 100B USD")
    print(filtered_100)
    return filtered_100


# Region별 상위 5개 국가의 GDP 평균 계산
def region_top5_calculate(df):
    region_top5_avg = (
        df.groupby('Region')
        .apply(lambda x: x.nlargest(5, 'GDP (B USD)')['GDP (B USD)'].mean())
        .reset_index(name='Top 5 Avg GDP (B USD)')
    )
    print("Average GDP of top 5 countries by region")
    print(region_top5_avg)
    return region_top5_avg


# 메인 ETL 함수
def etl_process():
    try:
        log_started()
        log_message("ETL Process Started")

        # 설정 로드
        url, table_class = load_config()

        # Extract
        extracted_data = extract_gdp_data(url, table_class)

        # Save Extracted Data
        save_gdp_data(extracted_data)

        # Transform
        transformed_data = transform_gdp_data(extracted_data)

        # Save Transformed Data
        save_gdp_data(transformed_data, 'transformed_gdp_data.csv', 'transformed_gdp_data.json')

        # Load into SQLite Database
        load_gdp_data(transformed_data)

        # Additional Analyses
        display_countries_with_gdp_over_100()
        display_region_top5_average_gdp()


        ## 추가요구사항 전 출력과정
        #filtered_data = filtered_100USD(transformed_data)
        #region_top5_data = region_top5_calculate(transformed_data)

        log_message("ETL Process Completed Successfully")
    except Exception as e:
        log_message(f"ETL Process Failed: {str(e)}", level="ERROR")


if __name__ == "__main__":
    etl_process()