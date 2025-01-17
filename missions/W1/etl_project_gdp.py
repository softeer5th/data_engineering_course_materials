'''
해당 파일의 코드 구성 방향과 이유는 W1M3 - ETL 프로세스 구현하기.ipynb를 참고.
'''

# ETL 프로세스 과정에서 사용되는 패키지
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import datetime as datetime
import pycountry_convert as pc

# 로그 기록 함수
def log(describe) :
    log_txt = open('etl_project_log.txt','a')
    print(datetime.datetime.now(), describe, '\n', file = log_txt)
    log_txt.close()

# GDP data Extract
def extract_gdp(raw_data_file_name):

    log("Extract start")
    
    # Wikipedia GDP web scraping
    # 대용량의 Raw data에 대비하여 stream = True로 설정
    html = requests.get("https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29", stream = True).text
    soup = BeautifulSoup(html, 'html.parser')
        
    # Wikipedia GDP 테이블 추출
    gdp_html = soup.find("table", {"class": 'wikitable'})
    df_raw = pd.read_html(StringIO(str(gdp_html)))[0]
    df_raw.to_json(raw_data_file_name)
    log("Extract finish")

# IMF GDP 테이블로 변환
def transform1_gdp(raw_data_file_name):
    
    log("Transform1 start")
    
    # 추출된 파일 열기
    df_gdp = pd.read_json(raw_data_file_name)
    
    # 테이블에서 국가열과 IMF GDP 열만 추출
    df_gdp = df_gdp.iloc[:,[0,1]].drop(0)

    # 열 이름 변경
    df_gdp.columns = ['country', 'GDP_USD_Million']

    # 해당 단계에서 Transform된 데이터 json 저장
    df_gdp.to_json('transform_gdp.json')

    log("Transform1 finish")

# GDP열을 1 Billion USD 단위로 변경
def transform2_gdp():
    
    log("Transform2 start")
    df_gdp = pd.read_json('transform_gdp.json')

    # gdp열의 자료형을 object에서 float로 변경
    df_gdp['GDP_USD_Million'] = df_gdp['GDP_USD_Million'].apply(lambda x : float(x) if x != '—' else None)

    # 단위 변경
    df_gdp['GDP_USD_Billion'] = round(df_gdp['GDP_USD_Million']/1000, 2)

    # Million 열 제거
    df_gdp = df_gdp.drop('GDP_USD_Million', axis = 1)

    df_gdp.to_json('transform_gdp.json')
    log("Transform2 finish")

# 국가 GDP가 높은 순서대로 정렬
def transform3_gdp():

    log("Transform3 start")
    df_gdp = pd.read_json('transform_gdp.json')

    # 정렬
    df_gdp = df_gdp.sort_values('GDP_USD_Billion', ascending = False)
    df_gdp.to_json('transform_gdp.json')
    log("Transform3 finish")

# 국가에 따른 region 열 추가 
def transform4_gdp():
    
    log("Transform4 start")
    df_gdp = pd.read_json('transform_gdp.json')
    
    # 국가에 따른 region 열 맵핑 함수
    def country_to_continent(x) :
        try : continent = pc.country_alpha2_to_continent_code(pc.country_name_to_country_alpha2(x))
        except :
            # pycountry-convert 패키지에 인식되지 않는 국가들의 region을 직접 매핑
            exception_region_dict = {'DR Congo' : 'AF', 'Kosovo' : 'EU', 'Sint Maarten' : 'EU', 'Zanzibar' : 'AF', 'East Timor' : 'AS'}
            continent = exception_region_dict[x]
        return continent

    # continent 열 추가
    df_gdp['continent'] = df_gdp['country'].apply(lambda x : country_to_continent(x))
    log("Transform4 finish")
    return df_gdp

# GDP data Transform
def transform_gdp(raw_data_file_name) :
    transform1_gdp(raw_data_file_name)
    transform2_gdp()
    transform3_gdp()
    transformed_gdp = transform4_gdp()
    return transformed_gdp
    
# GDP data Load
def load_gdp(df_gdp, load_data_file_name) :
    
    log("Load start")

    # json 파일로 저장하는 것을 Load로 설정.
    df_gdp.to_json(load_data_file_name)

    log("Load finish")

# GDP display
def display(load_data_file_name) :

    df_gdp = pd.read_json(load_data_file_name)

    # GDP가 100 Billion USD 이상인 행 출력
    df_100B = df_gdp[df_gdp['GDP_USD_Billion'] >= 100]
    print(df_100B)

    # 각 Region별로 top5 국가의 GDP 평균을 구해서 출력
    df_region_top5 = df_gdp.groupby('continent').head(5).groupby('continent').mean('GDP_USD_Billion').sort_values('GDP_USD_Billion', ascending = False)
    print(df_region_top5)

# GDP ETL process
def ETL_gdp(raw_data_file_name, load_data_file_name) :
    extract_gdp(raw_data_file_name)
    df_gdp_final = transform_gdp(raw_data_file_name)
    load_gdp(df_gdp_final, load_data_file_name)
    display(load_data_file_name)