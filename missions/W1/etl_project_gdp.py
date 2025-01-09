import requests
from bs4 import BeautifulSoup# 크롤링
import pandas as pd # 데이터 처리
import json # to json file
import datetime # logging
import sqlite3 # DB
import re
import datetime
import logging
from functools import wraps
from itertools import islice
import os

# logger setting
current_dir = os.path.dirname(os.path.abspath(__file__))

log_file = 'etl_project_log.txt'
log_file_path = os.path.join(current_dir, log_file)

raw_json = 'Countries_by_GDP.json'
raw_json_file_path = os.path.join(current_dir, raw_json)

db_name = 'World_Economies.db'
db_path = os.path.join(current_dir, db_name)

logger = logging.getLogger()
# 테스트 로그 메시지
# logger.info("This is a test log message.")
#========================================================================================

def is_within_5_months(past_year:int, past_month:int, latest_year:int, latest_month:int):
    '''연도와 월을 기준으로 총 차이가 5개월 이내인지 계산'''
    total_months_past = past_year * 12 + past_month
    total_months_latest = latest_year * 12 + latest_month
    
    return (total_months_latest - total_months_past) <= 5

#========================================================================================

def get_latest_log_with_pandas(file_path:str, seeker:int = -1):
    """
    Pandas를 사용해 로그 파일의 마지막 로그를 가져옵니다.
    로그 파일의 크기가 작다면 이쪽이 더 효율적입니다.
    로그는 '년도, 작업_성공_여부, 작업' 형태로 구조화되어 있어야 합니다.
    
    Args:
        file_path (str): 로그 파일 경로 (CSV 형식).
        seeker (int): 읽을 로그 인덱스
    Returns:
        str: "년도 (int), 작업_성공_여부 (str), 작업 (str)"
        int: 읽어온 로그의 직전 로그 인덱스
    """
    # 파일이 비어 있는지 확인
    with open(file_path, 'r') as file:
        file.seek(0, os.SEEK_END)
        isempty = file.tell() == 0
        file.seek(0)    # 파일 되감기
        if isempty: 
            return '', 0
        
    # 로그 파일을 Pandas DataFrame으로 읽기
    df = pd.read_csv(file_path, header=None, names=['year', 'status', 'task'])

    # 마지막 로그 가져오기
    last_log = df.iloc[seeker]
    return ",".join([last_log['year'], last_log['status'], last_log['task']]), seeker-1

#========================================================================================

def get_latest_log(file_path, start_position=None): 
    """
    가장 최근 기록된 로그를 가져오거나, 주어진 시작 위치에서 이전 로그를 탐색합니다.

    Args:
        file_path (str): 로그 파일 경로.
        start_position (int, optional): 탐색을 시작할 파일 포인터 위치. 기본값은 None(파일 끝).

    Returns:
        tuple: (buffer (str): 가장 최근 로그, file_position (int): 현재 파일 포인터 위치)
    """
    with open(file_path, 'rb') as file:
        # 파일 끝에서부터 탐색 시작
        if start_position is None:
            file.seek(0, 2)  # 파일 끝으로 이동
            file_position = file.tell()
        else:
            file.seek(start_position)
            file_position = start_position

        if file_position == 0:  # 파일이 비어 있거나 시작 위치가 파일 시작이라면
            return '', 0

        buffer = bytearray()
        while file_position > 0:
            file_position -= 1
            file.seek(file_position)
            byte = file.read(1)

            if byte == b'\n':  # 줄바꿈 문자를 만남
                if buffer:  # 버퍼에 내용이 있다면 최신 로그 반환
                    return buffer[::-1].decode('utf-8'), file_position
            else:
                buffer.append(byte[0])

        # 파일이 줄바꿈 없이 단일 라인인 경우
        if buffer:
            return buffer[::-1].decode('utf-8'), file_position

    return '', 0  # 비어 있는 파일

#========================================================================================
      
def is_DB_Latest(log: str):
    '''
    로그 기반으로 데이터 최신화 필요성을 검증합니다.
    마지막 데이터 저장 이후 5개월 초과 | 마지막 로그에 문제가 있었을 경우 False를 반환합니다.
    Args: log: str
    return: bool
    '''
    # !! IMF는 4, 10월에 데이터를 갱신한다 !!
    
    if len(log) == 0: return False
    now = datetime.datetime.now().strftime("%Y-%m")
    now_Y, now_m = map(int, now.split('-')[:2])
    
    log_year, log_month = log.split('-')[:2]
    log_year = int(log_year)
    log_month = int(datetime.datetime.strptime(log_month, '%B').month)

    return is_within_5_months(log_year, log_month, now_Y, now_m)

#========================================================================================
  
def url_validation_check(url:str):
    '''
    URL 유효성 검증 함수
    Args: url: str
    return: html text
    '''
    response = requests.get(url)

    if response.status_code == 200:
        return response.text
    else:
        print(response.status_code)  
        return False      

#========================================================================================

def flatten(arr:list):
    '''
    중첩 리스트 평탄화 함수
    Args: arr: list
    return: list
    '''
    return[
        item
        for sublist in arr for item in (flatten(sublist) 
        if isinstance(sublist, list)else [sublist])
    ]

#========================================================================================

class Extract:
    WIKI_URL = ''
    IMF_URL = ''
    now_year = int(datetime.datetime.now().strftime('%Y'))
    def __init__(self):
        self.WIKI_URL = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
        self.IMF_URL = 'https://www.imf.org/external/datamapper/api/v1/NGDPD'
    
    # def collect_data(self, html: str):
    #     '''
    #     원본 데이터 수집 함수
    #     국가별 자료입니다. 형식: [[국가명, GDP, 연도, GDP, 연도, GDP, 연도] ... ] 
    #     Args: html: str
    #     return: gdp_rows: List 
    #     '''
    #     soup = BeautifulSoup(html, 'html.parser')
    #     table = soup.select('table.wikitable > tbody > tr:nth-child(n+4)')#.wikitable sortable sticky-header-multi static-row-numbers jquery-tablesorter')
    #     # tr, td 등의 tag가 변수 취급이 가능하구나
    #     gdp_rows = [[td.get_text(strip=True) for td in tr.find_all('td')] for tr in table]

    #     return gdp_rows
    
    def data_from_IMF(self, year = now_year):
        '''
        현재 년도의 IMF의 API를 이용하여 데이터를 추출합니다.
        Args: year: int (현재년도)
        return: gdps: dict {country : GDP}
        '''
        year = year
        nameURL = 'https://www.imf.org/external/datamapper/api/v1/countries'
        gdpURL = f'https://www.imf.org/external/datamapper/api/v1/NGDPD?periods={year}'

        response = requests.get(nameURL)
        names = response.text
        namedict = json.loads(names)['countries'] # 국가 코드 : 국가명 딕셔너리
        # 여기까지 2초 ~ 3초

        rspn = requests.get(gdpURL)
        gdptext = rspn.text
        gdpdict = json.loads(gdptext)['values']['NGDPD']
        # 여기까지 3초 ~ 5초

        gdps = {}
        for k, v in gdpdict.items():
            if k in namedict.keys(): # gdpdict에는 국가 이외의 대륙 분류 등이 섞여있음
                # if year in v.keys():
                gdps[namedict[k]['label']] = round(v[year], 2)
                # else:
                    # gdps[namedict[k]['label']] = -1
        return gdps
    
    def save_json(self, url: str):
        '''
        웹 상의 테이블 데이터 '원본'을 JSON 형식으로 저장합니다.
        Args: url: str
        '''
        # 첫 번째 테이블을 가져옵니다 (여러 테이블이 있을 수 있으므로 인덱스로 선택)
        df = pd.read_html(url, attrs={"class": "wikitable"})[0]
         
        # MultiIndex 생성
        columns = [
            ["Country"] + ["IMF"] * 2 + ["WB"] * 2 + ["UN"] * 2,
            [   ""    ] + ["Forecast", "Year"] * 3  # 하위 컬럼: Forecast와 Year 반복
        ]

        # MultiIndex로 설정
        df.columns = pd.MultiIndex.from_arrays(columns)
                        
        try:
            df.to_json(raw_json_file_path, force_ascii=False, indent=4)
        except Exception as e:
            print("Error Occured While Saving Raw Data:", e)

#========================================================================================

class Transform:
    '''
    수정 완료 사항:
    타 클래스 의존성을 배제하여 원본 데이터 json 파일만 있다면 단독으로 동작 가능합니다.
    '''
    country = []
    gdp = []
    
    def __init__(self,):
        self.country = []
        self.gdp = []
        
    def refine_data(self, ):
        '''
        데이터를 정제합니다.
        링크를 지우고 문자열로 된 숫자를 정수로 변환합니다.
        '-'로 표기된 데이터는 None 을 기입합니다.
        
        return: country: List, gdp: List
        '''
        og_data = pd.read_json(raw_json_file_path)
        columns = [
            ["Country"] + ["IMF"] * 2 + ["WB"] * 2 + ["UN"] * 2,
            [""] + ["Forecast", "Year"] * 3  # 하위 컬럼: Forecast와 Year 반복
        ]
        og_data.columns = pd.MultiIndex.from_arrays(columns)     
        
        country = []
        gdp = []
        # year = []
        
        # to refine messy texts like link ... etc
        no_juseok = lambda s: re.sub(r'\[.*?\]', '', s)
        str_to_num = lambda s: int(re.sub(r',', '', s))
        
        for i, d in islice(enumerate(zip(og_data['Country'], og_data['IMF']['Forecast'])), 1, None):
            ctry, forecast = d[0], d[1]
            if ctry == None or forecast == None: 
                print(f"Data Error in line {i}")
                
            country.append(no_juseok(ctry))
            
            if forecast == '—':  # 결측치 처리
                gdp.append(None) # NULL value
            else: 
                gdp.append(str_to_num(no_juseok(forecast)))
        
        self.country = country 
        self.gdp = gdp
        
    def make_DataFrame(self, country:list, gdp:list):
        '''
        가공된 데이터로 데이터프레임을 생성합니다.
        GDP에 대해 내림차순 정렬되어 있고, 단위는 1B USD, 소수점 두자리까지 출력됩니다.
        Args: country: list[str], gdp: list[int | None]
        return: GDP_data: pd.DataFrame
        '''
        country = self.country
        gdp = self.gdp
        GDP_data = pd.DataFrame({'Country':country, 'GDP_USD_bilion':gdp})
        
        # GDP 내림차순 정렬
        GDP_data.sort_values(by=('GDP_USD_bilion'), ascending=False, inplace=True)
        
        # 소수점 둘째자리까지 보이기   
        GDP_data['GDP_USD_bilion'] = GDP_data['GDP_USD_bilion'].apply(lambda x: round(x / 1000, 2))
        
        return GDP_data      

#========================================================================================

class Load:
    frame = None
    conn = None
    def __init__(self, frame:pd.DataFrame, conn):
        '''
        데이터프레임을 저장하기 위해 가공된 데이터프레임을 불러옵니다.
        Args: frame: DataFrame, conn: db connection
        '''
        self.frame = frame    
        self.conn = conn    

    #========================================================================================

    def save_GDP_to_DB(self,):
        '''
        데이터프레임을 DB에 저장합니다.
        '''
        # try:
        #     with sqlite3.connect(db_path) as conn:
        #         print(f"Opened SQLite database with version {sqlite3.sqlite_version} successfully.")

        # except sqlite3.OperationalError as e:
        #     print("Failed to open database:", e)
        # cursor = conn.cursor()
        # cursor.execute("DROP TABLE IF EXISTS Countries_by_GDP;") #이미 있던 테이블 제거 후 저장
        try:
            self.frame = self.frame.sort_values(by='Country')
            self.frame.to_sql('Countries_by_GDP', self.conn, if_exists='replace')
        except Exception as e:
            print("Something Wrong With Saving Data to DB: ", e)
        
        self.conn.commit()
    #====================================================================================

    def save_Region_to_DB(self, region_data:dict):
        '''
        Region data를 DB에 'Region_Category' 테이블로 저장합니다.
        Args: region_data: dict
        '''
        region_list = []
        for region, countries in region_data.items():
            for country in countries:
                region_list.append({"Region": region, "Country": country})
        region_df = pd.DataFrame(region_list)  
              
        # try:
        #     with sqlite3.connect(db_path) as conn:
        #         print(f"Opened SQLite database with version {sqlite3.sqlite_version} successfully.")

        # except sqlite3.OperationalError as e:
        #     print("Failed to open database:", e)
            
        region_df.to_sql('Region_Category', self.conn, if_exists='replace')  
        self.conn.commit()     

#========================================================================================
         
def region_categorize(html: str):
    '''
    Region : [소속 국가] 형식의 딕셔너리를 반환합니다.
    Args: html: str
    return: region: dict
    '''
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.select('div.fancy > table > tr')

    category = [[td.get_text(strip=False) for td in tr.find_all('th')] for tr in table]
    for i in range(len(category) - 1, -1, -1):  # 인덱스를 뒤에서부터 순회
        if not category[i]: del category[i] # 리스트가 비어있으면 제거
        
    nara = [[td.get_text(strip=False) for td in tr.find_all('p')] for tr in table]
    for i in range(len(nara) - 1, -1, -1):  # 인덱스를 뒤에서부터 순회
        if not nara[i]: del nara[i] # 리스트가 비어있으면 제거
        
    region_arr = []
    for c in category:
        if len(c) == 0: continue
        region_arr.append(re.sub(r'[\r\n\xa0]', '', c[0]))
        
    for i, line in enumerate(nara):
        tmp = []
        for subline in line:
            tmp.append(subline.split('\n'))
        tmp = flatten(tmp)
        
        for j in range(len(tmp)-1, -1, -1):
            text = tmp[j]
            text = re.sub(r'\r', '', text)
            # '\xa0\xa0'은 ''으로 대체하고, '\xa0' 한 개는 ' '으로 대체
            # '\xa0\xa0' 앞뒤로 문자가 붙어있으면 공백으로 대체
            text = re.sub(r'(?<=[A-Za-z])\xa0\xa0(?=[A-Za-z])', ' ', text)            
            text = re.sub(r'(?<=[A-Za-z])\.(?=[A-Za-z])', ' ', text)
            text = re.sub(r'\xa0\xa0', '', text) 
            text = re.sub(r'\xa0', ' ', text) 
            tmp[j] = text.strip()    
            
            if text == '':
                del tmp[j]
                continue
        nara[i] = tmp
        
    region = {}
    for k, v in zip(region_arr, nara):
        region[k] = v
    
    return region        

#========================================================================================

def visualze_GDP_DESC_Over_100(gdp_data: dict):
    '''
    SQL을 사용하지 않고 GDP >= 100B USD인 데이터프레임을 출력합니다.
    Args: gdp_data: dict
    '''
    df = pd.DataFrame(gdp_data)
    df.sort_values('GDP_USD_bilion', ascending=False, inplace=True)
    df = df[df['GDP_USD_bilion'] >= 100]
    df['GDP_USD_bilion'] = df['GDP_USD_bilion'].apply(lambda x: round(x / 1000, 2))
    
    print(df)

#========================================================================================
    
def visualize_avg_GDP_by_Region(gdp_data:dict):
    '''
    SQL을 사용하지 않고 Region 별 상위 5개 국가의 평균 GDP를 기록한 데이터프레임을 출력합니다.
    Args: region: dict, gdp_data: dict
    '''
    region = region_categorize(url_validation_check('https://www.imf.org/external/datamapper/region.htm#sea'))

    # Step 1: region_to_country 데이터를 DataFrame으로 변환
    region_list = []
    for region, countries in region.items():
        for country in countries:
            region_list.append({"Region": region, "Country": country})
    region_df = pd.DataFrame(region_list)

     # Step 2: country_to_gdp 딕셔너리를 DataFrame으로 변환
    gdp_df = pd.DataFrame(gdp_data)
    # TODO --> None 값 -1로 수정해야 아래 코드가 돌아감
    gdp_df['GDP_USD_bilion'] = gdp_df['GDP_USD_bilion'].fillna(0)

    # Step 3: 두 DataFrame 병합 (Country를 기준으로)
    merged_df = pd.merge(region_df, gdp_df, on="Country", how="inner")

    
    # Step 4: Region 별로 그룹화
    merged_df.GDP_USD_bilion = merged_df.GDP_USD_bilion.astype(float)
    top_5_by_region = (
        merged_df.sort_values(by=['Region', 'GDP_USD_bilion'], ascending=[True, False])
        .groupby('Region')
        .head(5)
    )
    
    # Step 5: Region 별 상위 5개 GDP 평균 구하기
    region_avg_gdp = top_5_by_region.groupby("Region")["GDP_USD_bilion"].mean().reset_index().apply(lambda x: round(x, 2))
    region_avg_gdp.columns = ["Region", "@5_GDP_Avg"]
    
    print(region_avg_gdp)

#========================================================================================

def logging_time(func):
    '''
    @logging_time을 로깅하고 싶은 함수 윗줄에 작성한다.
    '''    
    @wraps(func)
    def wrapper(*args, **kwargs):
        # start_time = datetime.datetime.now()
        # formatted_start = start_time.strftime("%Y-%B-%d-%H-%M-%S")

        logging.info(f"Started, {func.__name__}")
        
        result = func(*args, **kwargs)
        
        # end_time = datetime.datetime.now()
        # formatted_end = end_time.strftime("%Y-%B-%d-%H-%M-%S")

        logging.info(f"Finished, {func.__name__}")
        # logging.info(f"Execution time for '{func.__name__}': {end_time - start_time}")
        return result
    return wrapper

#========================================================================================



#========================================================================================

class Executer:
    conn = None
    
    REFINED_DATA = None
    
    def __init__(self, conn:sqlite3.Connection):
        self.conn = conn
        self.REFINED_DATA = None
        
    #====================================================================================        
    
    @logging_time
    def do_Extract(self,):
        
        E = Extract() 
        wiki_html = url_validation_check(E.WIKI_URL) # 웹 페이지 추출
        # self.wiki_data = E.collect_data(wiki_html) # 데이터 수집
        E.save_json(wiki_html) # JSON 저장
        
    #====================================================================================
    
    @logging_time
    def do_Transform(self, ):
        
        T = Transform()
        T.refine_data()
        self.REFINED_DATA = T.make_DataFrame(T.country, T.gdp) # 데이터 정제 후 데이터프레임 반환
        
    #====================================================================================    
    
    @logging_time 
    def do_Load(self,):
        # dataframe = REFINED_DATA
        
        c = self.conn.cursor()
        tables = c.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        
        L = Load(self.REFINED_DATA, self.conn)
        L.save_GDP_to_DB()
        
        if 'Region_Category' in tables:
            pass
        else:
            url = 'https://www.imf.org/external/datamapper/region.htm#sea'
            L.save_Region_to_DB(region_categorize(url_validation_check(url)))
        
#========================================================================================
'''
TODO: gdp 저장 시 나라이름순 정렬
    : 갱신 시 오래된 데이터 따로 빼기
'''
if __name__ == "__main__":
    if not logger.handlers:
        # 사용자 정의 Formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s, %(message)s',
            datefmt='%Y-%B-%d-%H-%M-%S'  # 원하는 날짜 포맷
        )

        # 핸들러 생성 및 설정
        handler = logging.FileHandler(log_file_path)
        handler.setFormatter(formatter)

        # 로거 설정
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    
    buffer, fp = get_latest_log_with_pandas(log_file_path)

    while True: # 가장 마지막으로 수행 완료된 Load 프로세스 로그 탐색
        if len(buffer) == 0:  # 로그가 비어 있으면 종료
            # print("로그 끝에 도달했지만 조건에 맞는 항목을 찾지 못했습니다.")
            break

        # 최신 로그에서 `마지막으로 수행된 프로세스`와 `프로세스 완료 여부` 추출
        try:
            completion, process = buffer.split(', ')[-2:]  # 로그가 예상 형식일 경우
        except ValueError:
            print(f"잘못된 로그 형식: {buffer}")
            break

        # 조건 충족 시 종료
        if completion == 'Finished' and process == 'do_Load':
            # print(f"조건을 충족하는 로그 발견: {buffer}")
            break

        # 이전 로그 탐색
        buffer, fp = get_latest_log_with_pandas(log_file_path, seeker=fp)
        

    # 최신 데이터 -> 갱신 과정 없이 데이터 제공
    if  is_DB_Latest(buffer): 
        print("You're looking latest DB")
        
        runner = Executer(conn = None)
        runner.do_Transform()
        
        print(f"{'GDP Over 100B USD':-^34}")
        print()
        visualze_GDP_DESC_Over_100(runner.REFINED_DATA)
        print()
        print(f"{'Average GDP of Each Region':-^44}")        
        print()
        visualize_avg_GDP_by_Region(runner.REFINED_DATA)
    
    # 오래된 데이터 -> 갱신 필요    
    else:
        print("DB is outdated: Start Update ...")

        runner = Executer(conn = None) # Load process 전 까진 DB 연결할 일이 없다.
        
        runner.do_Extract()
        # print("Extracted")
        runner.do_Transform()
        # print("Transformed")
        
        try:    # check DB Connection
            print("Connecting DB ...")
            
            with sqlite3.connect('World_Economies.db') as conn:
                print(f"Opened SQLite database with version {sqlite3.sqlite_version} successfully.")
                c = conn.cursor()

        except sqlite3.OperationalError as e:
            print("Failed to open database:", e)   

        runner.conn = conn
        runner.do_Load()
        # print("Loaded")
        
        print(f"{'GDP Over 100B USD':-^34}")
        print()
        visualze_GDP_DESC_Over_100(runner.REFINED_DATA)
        print()
        print(f"{'Average GDP of Each Region':-^44}")        
        print()
        visualize_avg_GDP_by_Region(runner.REFINED_DATA)
        
        conn.commit()
    
 
