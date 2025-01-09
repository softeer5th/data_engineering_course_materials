from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import pycountry_convert
import pycountry
import time
from multiprocessing import Pool
from multiprocessing import set_start_method
import multiprocessing as mp
import numpy as np
import os

# datetime을 활용하여 구현한 log 파일을 찍는 클래스
# start를 하면 로그파일이 열리게 되고, end를 하면 로그를 찍는것을 중단하게 된다.
class Logger:
    def __init__(self):
        self.logfile_name = 'etl_project_log.txt'
        self.log_format = '{timestamp}, {message}'
        self.timestamp_format = '%Y-%b-%d-%H-%M-%S'
        self.file = None

    def start(self):
        if self.file is None :
            self.file = open(self.logfile_name,mode='a')
            
    def end(self):
        if self.file :
            self.file.write("===============================================" + "\n\n")
            self.file.close()
            self.file = None

    def get_timestamp(self):
        return datetime.now().strftime(self.timestamp_format)
    
    def info(self,message):
        if self.file is None :
            raise RuntimeError()

        timestamp = self.get_timestamp()
        formatted_message = self.log_format.format(timestamp=timestamp, message=message)
        self.file.write(formatted_message + "\n")
        

# web scaping 방식을 통해 wikipeida 홈페이지에서 데이터 추출 하는 함수
# raw_data를 json 형태로 저장
def extract_data():
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
    raw_data = []

    with requests.get(url, stream = True) as response:
        response.raise_for_status()
        with open('file.html','wb') as file :
            for chunk in response.iter_content(chunk_size = 8192):
                if chunk:
                    file.write(chunk)
        
    with open('file.html','rb') as file:
        html = file.read()
            
    # 문자열로 된 객체를 python object로 변환하는 과정
    soup = BeautifulSoup(html, 'html.parser')
    tbody = soup.select('table.wikitable > tbody')

    for table in tbody:
        rows = table.find_all('tr')
        for row in rows:
            columns = row.find_all('td')
            if len(columns) > 1:
                # <sup> 태그와 같은 필요없는 태그 제거하기
                for column in columns:
                    for tag in column.find_all('sup'):
                        tag.decompose()
                    
                country = columns[0].get_text(strip = True)
                gdp = columns[1].get_text(strip = True)
                year = columns[2].get_text(strip = True)

                # 없는 데이터 처리하기
                if gdp == '—' :
                    gdp = None
                    year = None

                raw_data.append([country, gdp, year])
                        
    df = pd.DataFrame(raw_data, columns = ["Country", "GDP_USD_million", "Year"])
    df.to_json('Countries_by_GDP.json', orient = 'index', indent = 4)


# 국가별 GDP 단위를 million USD -> billion USD 변환
# 각 나라의 Region을 찾아 입력
def transform_data(df):
    print("transform_data : PID {pid}".format(pid = os.getpid()))
    df["GDP_USD_billion"] = df["GDP_USD_million"].replace(",","",regex = True).astype(float, errors="ignore").div(1000).round(2)
    df["Region"] = df["Country"].apply(convert_name_to_continent)

    df = df.drop("GDP_USD_million", axis = 1)
    return df

def convert_name_to_continent(country_name):
    try :
        alpha2_code = pycountry_convert.country_name_to_country_alpha2(country_name)
    
        if alpha2_code :
            continent_code = pycountry_convert.country_alpha2_to_continent_code(alpha2_code)
            continent_name = pycountry_convert.convert_continent_code_to_continent_name(continent_code)
            return continent_name
        else : 
            return None

    except Exception :
        # pycountry 에서 지원하지 않는 나라의 이름은 매핑 딕셔너리 이용해서 mapping
        custom_country_mapping = {
            "Kosovo" : "Europe",
            "DR Congo" : "Africa",
            "Zanzibar" : "Africa",
            "East Timor" : "Asia",
            "Sint Maarten" : "North America"
        }

        if country_name in custom_country_mapping:
            return custom_country_mapping[country_name]
        else :
            return None
        
def load_data(dataframe):
    print()
    print_screen(dataframe)

# GDP가 100B USD 이상인 국가와 
# 각 Region별로 top5 국가의 GDP 평균을 출력하는 함수.
def print_screen(df):
    print("<< GDP가 100B USD이상이 되는 국가만 출력 >>")
    print()
    print(df[df['GDP_USD_billion'] >= 100])

    print()
    print("<< 각 Region별로 top5 국가의 GDP 평균>>")
    print()
    grouped = df.groupby('Region')
    print(grouped.apply(lambda g : g.sort_values(by='GDP_USD_billion', ascending = False)[:5]['GDP_USD_billion'].mean(), include_groups = False))   

def parallel_dataframe(df, func, num_cores = 8):
    df_split = np.array_split(df, num_cores)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

# 전체 과정의 함수
# 각각의 과정 속에서 로그를 찍음.
# 데이터의 사이즈가 클 경우를 대비해 parallel_dataframe 메소드를 사용하여 
# 멀티프로세싱으로 데이터 transform을 진행
def ETL():
    logger = Logger()
    logger.start()

    logger.info("Extract started")
    # extract_data()
    logger.info("Extract completed")
    
    logger.info("Transform started")
    start = time.time()
    df = pd.read_json('Countries_by_GDP.json', orient = 'index')
    transformed_data = parallel_dataframe(df, transform_data)
    # transformed_data = transform_data(df)
    end = time.time()
    logger.info("Transform completed")

    print("수행시간 : {time} 초".format(time = end-start))

    logger.info("Load started")
    load_data(transformed_data)
    logger.info("Load completed")

    logger.end()

if __name__ == "__main__":
    ETL()