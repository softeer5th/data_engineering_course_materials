import requests
import datetime as dt
import json
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3

class Extract:
    def __init__(self, url, path):
        self.url = url
        self.path = path
        self.response = None
        self.raw_data = None

    # Name: run
    # Parameter: Extract self
    # return: raw data extracted
    def run(self):
        writeLog('Extract Start', PATH_LOG)
        self.response = Extract.get_response_from_url(self.url)
        self.raw_data = Extract.save_raw_data(self.response, self.path, self.url)
        writeLog('Extract finished', PATH_LOG)
        return self.raw_data

    # Name: get_response_from_url
    # Parameter: url to get response
    # Return: response
    def get_response_from_url(url):
        try:
            response = requests.get(url) # Request from url
            response.raise_for_status() # Response status chekcing
            return response
        except requests.RequestException as e: # 리스폰스 에러 처리
            print(f'Error: request: {e}')
            return None

    # Name: save_raw_data
    # Parameter: response to save, path to save, url extracted
    # Return: dict raw_data
    def save_raw_data(response, path, url):
        # response check
        if response == None: 
            print('Error: response is None')
            return None

        now = dt.datetime.now().strftime('%Y-%b-%d-%H-%M-%S') # 현재 시간 문자열로 저장
        # raw data: url, text, date
        raw_data = {'url' : url,
                    'text' : response.text,
                    'date' : now}
        
        # try to save raw data on path
        try:
            with open(path, 'w') as f: # path에 raw_data json형식으로 덤프
                json.dump(raw_data, f)
            return raw_data
        except Exception as e:
            print(f'Error: raw data: {e}')
            return None
            
# Transform
# 포매팅. 테이블을 뽑아내자
class Transform:

    # Name: Transform
    # Parameter: dictionary raw data
    # Return: Transform
    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.table = None
        self.soup = None
        self.df = None
    
    # Name: get_soup
    # Parameter: raw data
    # Return: soup parsed by lxml
    def get_soup(raw_data):
        soup = BeautifulSoup(raw_data['text'], 'lxml') # lxml로 raw data 파싱
        return soup
    
    # Name: get_table
    # Parameter: soup, css_selector_type, css_selector
    # Return: Table(bs4 object)
    def get_table(soup, css_selector_type, css_selector):
        type_dict = {'class' : '.', 'ID' : '#', 'tag_name' : '' } # css Selector dictionary

        # 타입 유효성 검사
        if css_selector_type not in type_dict:
            print('Error: css_selector_type: Wrong css_selector_type')
            return None
        
        # 타겟 타입 클래스인 경우 스페이스바 다 '.'으로 바꾸어서 셀렉트
        if css_selector_type == 'class':
            css_selector = css_selector.replace(' ', '.')

        # 타입 셀렉터로 셀렉트
        table = soup.select_one(type_dict[css_selector_type] + css_selector)
        return table

    def get_dataframe(table):
        df = pd.read_html(str(table))[0] # read_html은 list 반환해서 첫 번쨰 테이블 지정
        return df
    
    def filter_dataframe(df, droplevel = None, column_nums = [], row_dropna = False, row_drophyphen = False, column_names = [], astype = {}, reset_index = False):

        # column droplevel
        if droplevel is not None:
            df.columns = df.columns.droplevel(droplevel)
        
        # 선택할 열을 리스트로 받아 해당 열만 남김
        if column_nums:
            df = df.iloc[:,(column_nums)]

        # row_dropna True면 NaN 로우 다 제거
        if row_dropna:
            df.dropna(axis = 0, inplace = True)
        
        # hyphen 값을 가지는 모든 로우 제거
        if row_drophyphen:
            df.replace('—', pd.NA, inplace = True)
            df.dropna(axis = 0, inplace = True)

        # 열 이름 받으면 리네임
        if column_names:
            df.columns = column_names
        
        # 열 타입 지정
        if astype:
            df = df.astype(astype)

        # reset index
        if reset_index:
            df = df.reset_index(drop = True)

        return df

    # 변환 수행
    def run(self, css_selector_type, css_selector, raw_data = None, droplevel = None, column_nums = [], row_dropna = False, row_drophyphen = False, column_names = [], astype = {}, reset_index = False):
        if raw_data:
            self.raw_data = raw_data
        self.soup = Transform.get_soup(self.raw_data)
        self.table = Transform.get_table(self.soup, css_selector_type, css_selector)
        self.df = Transform.get_dataframe(self.table)
        self.df = Transform.filter_dataframe(self.df, droplevel = droplevel, column_nums= column_nums, row_dropna = row_dropna, row_drophyphen = row_drophyphen, column_names = column_names, astype = astype, reset_index = reset_index)
        return self.df


class TransformGDP(Transform):
    # Transform 클래스를 상속받음

    # 생성자.
    def __init__(self, raw_data, path_region):
        self.path_region = path_region
        super().__init__(raw_data)

    # 단위 변환
    def bil_to_mil(self):
        self.df['GDP_USD_billion'] = (self.df['GDP_USD_billion'] / 1000).round(2)
        return self.df

    # 지역 열 추가
    def add_region_column(self, df, path_region):
        region_df = pd.read_csv(path_region)
        region_df = pd.DataFrame({'Country' : region_df['name'], 'Region':region_df['region']})
        self.df = pd.merge(df, region_df, how='left', on='Country')
        self.df = self.df.dropna() # 지역이 없는 행 제거
        return self.df

    # 변환 수행
    def run(self, path_region, css_selector_type, css_selector, droplevel=None, column_nums=[], row_dropna=False, row_drophyphen=False, column_names=[], astype={}, reset_index=False,):
        writeLog('Transform Start', PATH_LOG)
        # 부모 클래스의 run 메서드 호출 시 매개변수 전달
        self.df = super().run(
            css_selector_type=css_selector_type,
            css_selector=css_selector,
            droplevel=droplevel,
            column_nums=column_nums,
            row_dropna=row_dropna,
            row_drophyphen=row_drophyphen,
            column_names=column_names,
            astype=astype,
            reset_index=reset_index
        )
        self.bil_to_mil()
        self.add_region_column(self.df, path_region)
        writeLog('Transform Finished', PATH_LOG)
        return self.df

# Load
class Load:
    def __init__(self, df, path_db):
        self.df = df
        self.path_db = path_db
        self.con = None
        self.cursor = None
        self.sql = None
    
    def open_con(self, path_db = None):
        if path_db:
            self.path_db = path_db
        self.con = sqlite3.connect(self.path_db)
        self.cursor = self.con.cursor()
        return self.cursor

    def create_table(self, df_name, df = None, index = None):
        if df:
            self.df = df

        if index == False:
            self.df.to_sql(name = df_name, con = self.con, if_exists = "replace", index = False,)
        else:
            self.df.to_sql(name = df_name, con = self.con, if_exists = "replace",)
        self.con.commit()

    def close_con(self, con = None):
        if con:
            self.con = con
        self.con.close()

    def select_sql(self, sql):
        if sql:
            self.sql = sql
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        
        return rows

        
class LoadGDP(Load):
    def __init__(self, df, path_db, ):
        super().__init__(df, path_db)

    def print_GDP_over_100B(self):
        print("\nGDP가 100B 이상인 국가들")
        sql = '''
        SELECT Country, GDP_USD_billion
        FROM Countries_by_GDP
        WHERE GDP_USD_billion > 100
        '''
        rows = LoadGDP.select_sql(self, sql)
        print("Country                     | GDP_USD_billion")
        for row in rows:
            country = "%-20s" % row[0]
            GDP = row[1]
            print(country + " |", GDP)
        
        return rows
    
    def print_avg_Top_N_GDP_by_region(self, n):
        print("\n지역별 탑5 국가의 GDP 평균")
        regions = ['Africa', 'Americas', 'Asia', 'Europe', 'Oceania']
        data = []
        print("Region   | Avg of Top 5")
        for region in regions:
            sql = '''
            SELECT AVG(GDP_USD_billion)
            FROM (
            SELECT GDP_USD_billion
            FROM Countries_by_GDP
            WHERE Region = "''' + region +  '''"
            ORDER BY GDP_USD_billion DESC
            LIMIT 5
            )
            '''
            form = [region, LoadGDP.select_sql(self, sql)[0][0]]
            data.append(form)
            print('%-10s' % form[0], form[1])

        return data

# GDP 100B 이상인 국가 데이터프레임
def get_GDP_over_100B(df):
    df = df[df.GDP_USD_billion > 100]
    return df

# 지역별 GDP 탑 5 국가의 GDP 평균
def get_top_n_avg_gdp_by_region(df, top_n=5):
    print("지역별 탑 5의 GDP 평균")
    sorted_df = df.sort_values(by=['Region', 'GDP_USD_billion'], ascending=[True, False])
    top_GDP = sorted_df.groupby('Region').head(top_n)
    avg_GDP = top_GDP.groupby('Region')['GDP_USD_billion'].mean().reset_index()
    avg_GDP.sort_values(by=['GDP_USD_billion'], ascending = False, inplace = True)
    return avg_GDP

# writeLog
def writeLog(log, path_log):
    now = dt.datetime.now()
    log = now.strftime("%Y-%b-%d-%H-%M-%S, ") + log + "\n"
    with open(path_log, 'a') as f:
        f.write(log)


# const block
URL = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
PATH_RAW_DATA = 'missions/W1/M3/data/Countries_by_GDP.json'
PATH_REGION = '/Users/speowo/workspace/Wiki-HMG/missions/W1/M3/data/region.csv'
PATH_LOG = '/Users/speowo/workspace/Wiki-HMG/missions/W1/M3/data/etl_project_log.txt'
PATH_DB = 'missions/W1/M3/data/World_Economies.db'
CSS_SELECTOR_TYPE = 'class'
CSS_SELECTOR = 'wikitable sortable sticky-header-multi static-row-numbers'
TABLE_NAME = 'Countries_by_GDP'

# main

# Extract
e = Extract(URL, PATH_RAW_DATA)
raw_data = e.run()

# Transform
t = TransformGDP(raw_data, PATH_REGION)
df = t.run(PATH_REGION, CSS_SELECTOR_TYPE, CSS_SELECTOR, 
            droplevel = 0, 
            column_nums = [0, 1], 
            row_dropna= True, 
            row_drophyphen = True, 
            column_names=['Country', 'GDP_USD_billion'], 
            astype={'GDP_USD_billion' : 'float'},
            reset_index = True)

# Load
l = LoadGDP(df, PATH_DB)
writeLog("Load Start", PATH_LOG)
l.open_con(PATH_DB)
l.create_table(TABLE_NAME)
l.close_con()
writeLog("Load finished")

l.open_con(PATH_DB)
l.print_GDP_over_100B()
l.print_avg_Top_N_GDP_by_region(5)
l.close_con()

# Result
# print(get_GDP_over_100B(df))
# print(get_top_n_avg_gdp_by_region(df))
