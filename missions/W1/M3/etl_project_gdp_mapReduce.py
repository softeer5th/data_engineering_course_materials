# MapReduce를 활용한 병렬/분산 처리 방식을 도입해 
# 각 Region별로 top5 국가의 GDP 평균을 출력하기

from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import pycountry_convert
import time
from multiprocessing import Pool
from collections import defaultdict
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

# 국가와 대륙을 매핑해주는 함수
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
        


# 각 Region별로 top5 국가의 GDP 평균을 화면에 출력하는 함수.
def print_screen(transformed_data):

    print()
    print("<< 각 Region별로 top5 국가의 GDP 평균>>")
    print()
        
    for item in transformed_data:
        if item:
            for region, value in item.items():
                if region is not None:
                    print(f"Region: {region}, GDP: {value}")


# MapReduce 방식을 이용한 병렬처리
# MapReduce 과정을 파이썬으로 구현
# Map과 Reduce 모두 병렬/분산 처리로 진행된다. 
def parallel_mapreduce(df, num_cores = 8):
    df_split = np.array_split(df, num_cores)
    # Map 과정, 국가별 Region 정해주기 및 GDP 단위 변경하기
    with Pool(num_cores) as pool :
        mapped_data = sum(pool.map(map_function ,df_split),[])

    # Shuffling 과정, Region을 key로 하여 value list 만들기
    shuffled_data = shuffle_function(mapped_data)

    # Reduce 과정, 각 Region 별 top5  GDP 평균 내기
    data_split = split_dictionary(shuffled_data, num_cores)
    with Pool(num_cores) as pool : 
        result = pool.map(reduce_function, data_split)
    return result

# Map과정 
# billion USD 단위로 처리해주고, 지역을 매핑한 후 key, value 쌍으로 return
# output : list(key, value)
def map_function(df) :
    df["GDP_USD_billion"] = df["GDP_USD_million"].replace(",","",regex = True).astype(float, errors="ignore").div(1000).round(2)
    df["GDP_USD_billion"] = df["GDP_USD_billion"].fillna(0)
    df["Region"] = df["Country"].apply(convert_name_to_continent)

    key_value_pairs = []
    for _, row in df.iterrows():
        key_value_pairs.append((row["Region"], row["GDP_USD_billion"]))

    return key_value_pairs 

# Shuffle 과정
# Region을 key로 하여 value list 만드는 함수
# output : list(key,list(value))
def shuffle_function(mapped_data):
    key_dict = defaultdict(list)
    for data in mapped_data:
        key, value = data[0], data[1] 
        key_dict[key].append(value)
    return key_dict

# Reduce 과정
# value들을 이용해 top5 내의 GDP 평균값을 구하는 함수
# output : list(key, value)
def reduce_function(reduced_data):
    result = defaultdict(float)    
    if reduced_data != []:
        for region, values in reduced_data[0].items() :
            sorted_values = sorted(values, reverse=True)
            result[region] = np.mean(sorted_values[0:5])
    return result

def split_dictionary(input_dict, num_core):
    items = list(input_dict.items())
    split_dicts = [[] for _ in range(num_core)]
    for i, (key, value) in enumerate(items):
        split_dicts[i % num_core].append({key: value})
    
    return split_dicts


# 전체 과정의 함수
# 각각의 과정 속에서 로그를 찍음.
# 데이터의 사이즈가 클 경우를 대비해 parallel_mapreduce 메소드를 사용하여 
# MapReduce 방식의 분산컴퓨팅으로 구현

# - 한계 
# 억지로 MapReduce원리를 파이썬 내에서 따라하려고 하다 보니,
# list, dictionary와 같은 타입의 변환이 계속해서 이뤄지고 있다. 
def ETL():
    file_name = 'Countries_by_GDP.json'
    logger = Logger()
    logger.start()

    try :
        if not os.path.exists(file_name):
            raise FileNotFoundError()
    except :
        # raw_data 파일이 없으면 extract 시작
        logger.info("Extract started")
        extract_data()
        logger.info("Extract completed")
    
    logger.info("Transform started")
    start = time.time()
    df = pd.read_json('Countries_by_GDP.json', orient = 'index')
    transformed_data = parallel_mapreduce(df)
    end = time.time()
    logger.info("Transform completed")

    print_screen(transformed_data)
    print("수행시간 : {time} 초".format(time = end-start))
    logger.end()

if __name__ == "__main__":
    ETL()