from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import json
import pycountry_convert as pc

## 로그 기록 함수
def log_message(message, log_file="./etl_project_log.txt"):
    # 현재 시간 가져오기
    current_time = datetime.now().strftime('%Y-%B-%d-%H-%M-%S')
    
    # 로그 형식 생성
    log_entry = f"{current_time}, {message}\n"
    
    # 로그를 파일에 추가 기록
    with open(log_file, 'a', encoding='utf-8') as file:
        file.write(log_entry)

    # 콘솔에도 출력 
    print(log_entry.strip())


## 웹에서 정보 가져와서 json 파일로 저장
## 파라미터: 크롤링할 웹페이지의 url과 raw data저장할 파일명 
def extract_json(url, file_name):

    # URL에서 HTML 문서 가져오기
    response = requests.get(url)
    log_message("Fetched HTML from the URL.")

    # BeautifulSoup 인스턴스 생성
    soup = BeautifulSoup(response.text, 'html.parser')
    log_message("Parsed HTML with BeautifulSoup.")
    # 특정 클래스를 가진 테이블 가져오기
    table = soup.find('table', {'class': 'wikitable'})
    if table:
        log_message("Found the GDP table in the HTML.")
    else:
        log_message("Failed to find the GDP table in the HTML.")
        exit()

    headers = ['Country', 'GDP']

    # 테이블 행과 열 추출
    rows = []
    for tr in table.find_all('tr')[3:]:  # 헤더 행들 제외
        cells = [td.text.strip() for td in tr.find_all('td')]
        if cells:  # 빈 행 건너뛰기
            rows.append(cells[0:2]) #원하는 데이터만 가져오기
    log_message(f"Extracted {len(rows)} rows from the table.")

    # 데이터를 JSON 형태로 변환
    data = [dict(zip(headers, row)) for row in rows]

    # JSON 파일로 저장
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)


## 데이터셋 정제
def transform_data(file_name):
    # Pandas DataFrame으로 변환
    df = pd.read_json(file_name) ## pd.read_json 이용
    #df = pd.DataFrame(rows, columns=headers)
    log_message("Converted data to Pandas DataFrame.")

    df['GDP'] = df['GDP'].str.replace(',', '', regex=True)
    df['GDP'] = df['GDP'].replace('—', '0')
    df['GDP'] = df['GDP'].astype(int)
    df['GDP'] = df['GDP'].replace(0, np.nan)
    df['GDP_USD_billion'] = np.nan
    df['GDP_USD_billion'] = round(df['GDP'] * 0.001, 2)
 
    log_message("Cleaned and transformed the GDP data.")
    return df[['Country','GDP_USD_billion']] #단위 바꾼 GDP만 반환한다.

## 데이터 적재
## 파라미터: 저장할 데이터프레임, 저장할 파일 경로와 이름
def load_data(df,file_path):
    try:
        df.to_csv(file_path,index=False)
        log_message("Data successfully saved")
    except Exception as e:
        log_message("Data is not saved")

# 나라->Region 변환 함수
def country_to_continent(country_name):
    try:
        # 국가 이름을 대륙 이름으로 변환
        country_alpha2 = pc.country_name_to_country_alpha2(country_name)
        country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
        country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
        return country_continent_name
    except KeyError:
        # KeyError 발생 시 NaN 반환
        return np.nan


## 화면 출력(GDP가 100B USD이상이 되는 국가만)
def show_data1(df, val):

    df1 = df.sort_values(by=['GDP_USD_billion'], ascending=[False])
    cond = df1['GDP_USD_billion'] > val
    df1 = df1[cond]
    log_message(f"df1 dataset contains {len(df1)} rows.")
    print(df1)

## 화면 출력(각 Region별로 top5 국가의 GDP 평균을 구해서)
def show_data2(df):
    df['Region']=np.nan
    # 벡터화 처리
    df['Region'] = df['Country'].apply(country_to_continent)
    # GDP 기준으로 내림차순 정렬
    sorted_df = df.sort_values(by="GDP_USD_billion", ascending=False)

    # 각 Region별 상위 5개 국가 선택
    top5_per_region = (
        sorted_df.groupby("Region", group_keys=False)
                .apply(lambda x: x.nlargest(5, "GDP_USD_billion"))
    )

    # Region별 상위 5개 국가의 GDP 평균 계산
    region_top5_avg_gdp = top5_per_region.groupby("Region")["GDP_USD_billion"].mean()
    
    # 결과 출력
    print(region_top5_avg_gdp)
    log_message(f"df2 dataset contains {len(region_top5_avg_gdp)} rows.")


if __name__ == '__main__':

    log_message("Start ETL Process")

    #url->html->table에서 정보 추출해서 json 파일로 저장
    extract_json(url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29", file_name="./Countries_by_GDP.json")

    #json파일 불러와서 데이터 정제
    df = transform_data("./Countries_by_GDP.json")
    
    #Load data
    load_data(df, "./GDP_USD_billion.csv")
    
    #정제한 데이터 바탕으로 데이터 가공하여 출력
    show_data1(df,100)
    show_data2(df)

    log_message("ETL process completed successfully.")
