# 로그 기록 함수
def log(describe) :
    import datetime as datetime
    log_txt = open('etl_project_log.txt','a')
    print(datetime.datetime.now(), describe, '\n', file = log_txt)
    log_txt.close()

# GDP data Extract
def extract_gdp():
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
    from io import StringIO

    log("Extract start")
    
    # Wikipedia GDP web scraping
    # 대용량의 Raw data에 대비하여 stream = True로 설정
    html = requests.get("https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29", stream = True).text
    soup = BeautifulSoup(html, 'html.parser')

    # Wikipedia GDP 테이블 추출
    gdp_html = soup.find("table", {"class": 'wikitable'})
    df_raw = pd.read_html(StringIO(str(gdp_html)))[0]
    df_raw.to_json('raw_data_gdp.json')

    log("Extract finish")

    return 'raw_data_gdp.json'

# GDP data Transform
def transform_gdp(json):

    import pandas as pd
    import pycountry_convert as pc

    log("Transform start")

    # 추출된 파일 열기
    df_gdp = pd.read_json(json)

    # 테이블에서 국가열과 IMF GDP 열만 추출
    df_gdp = df_gdp.iloc[:,[0,1]].drop(0)

    # 열 이름 변경
    df_gdp.columns = ['country', 'GDP_USD_billion']

    # GDP열을 1 Billion USD 단위로 변경
    df_gdp['GDP_USD_billion'] = df_gdp['GDP_USD_billion'].apply(lambda x : float(x) if x != '—' else None)
    df_gdp['GDP_USD_billion'] = round(df_gdp['GDP_USD_billion']/1000, 2)

    # 국가 GDP가 높은 순서대로 정렬
    df_gdp = df_gdp.sort_values('GDP_USD_billion', ascending = False)

    # 국가에 따른 대륙열 추가 함수
    def country_to_continent(x) :
        try : continent = pc.country_alpha2_to_continent_code(pc.country_name_to_country_alpha2(x))
        except :
            # pycountry-convert 패키지에 인식되지 않는 국가들의 region을 직접 매핑
            exception_region_dict = {'DR Congo' : 'AF', 'Kosovo' : 'EU', 'Sint Maarten' : 'EU', 'Zanzibar' : 'AF', 'East Timor' : 'AS'}
            continent = exception_region_dict[x]
        return continent

    # 국가에 따른 대륙열 추가
    df_gdp['continent'] = df_gdp['country'].apply(lambda x : country_to_continent(x))

    log("Transform finish")

    return df_gdp

# GDP data Load
def load_gdp(df_gdp) :

    import datetime as datetime
    import pandas as pd
    import pycountry_convert as pc
    import sqlite3
    
    log("Load start")

    # 데이터베이스 연결
    conn = sqlite3.connect('World_Economies.db')

    # 데이터베이스에 데이터 적재
    df_gdp.to_sql('Countries_by_GDP', conn, index = False, if_exists='replace')

    # 커서 생성
    cur = conn.cursor()

    # GDP가 100 Billion USD 이상인 행 추출 함수
    def exceed_100B() :
        conn = sqlite3.connect('World_Economies.db')
        cur = conn.cursor()
        df = pd.read_sql("SELECT * FROM Countries_by_GDP WHERE GDP_USD_billion > 100", conn)
        return df

    print(exceed_100B())

    # 각 Region별로 top5 국가의 GDP 평균을 구하는 함수
    def region_top5_average() :
        conn = sqlite3.connect('World_Economies.db')
        cur = conn.cursor()
        df = pd.read_sql(
            """SELECT AVG(GDP_USD_billion) [TOP5 AVERAGE], CONTINENT
            FROM (SELECT * FROM 
            (SELECT *, ROW_NUMBER() OVER (PARTITION BY CONTINENT ORDER BY GDP_USD_billion DESC) AS RANK 
            FROM Countries_by_GDP) 
            WHERE RANK <=5)
            GROUP BY CONTINENT
            ORDER BY [TOP5 AVERAGE] DESC
            """, conn)
        return df

    print(region_top5_average())

    log("Load finish")

# GDP ETL process
def ETL_gdp() :
    json = extract_gdp()
    df_gdp = transform_gdp(json)
    load_gdp(df_gdp)

ETL_gdp()