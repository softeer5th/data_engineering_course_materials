import requests
import json
import pandas as pd
import pycountry_convert
import sqlite3
import os
import datetime as dt

# Extract
# 1. GDP json API request
# Parameter: X
# return json file of NGDPD 
def getGDPJsonFromIMF():
    response = requests.get("https://www.imf.org/external/datamapper/api/v1/NGDPD") # IMF에서 GDP API 요청
    path = "missions/W1/M3/Countries_by_GDP_from_IMF.json"
    check = open(path) # 변경 사항 검사
    if check.read() != response.text:
        archiveGDPJson(response)    # raw data 아카이빙
        with open(path, "w") as f:   # GDP json file로 저장
            f.write(response.text)

    js = response.json()
    return js

# 1-1 json archiving
# GDP 파일 버전 별로 저장
# Parameter: Response object from IMF NGDPD API
# return X
def archiveGDPJson(response):
    version = 1
    path = "missions/W1/M3/archive/Countries_by_GDP_from_IMF"
    while (os.path.isfile(path + str(version) + ".json")):
        version += 1
    path = path + str(version) + ".json"
    with open(path, "w") as f:   # GDP json file로 저장
        f.write(response.text)

# 2. Country Name Dictionary API request
# {나라 코드: 나라명 딕셔너리}를 API request해서 받아옴
# Parameter: X
# return: 나라 Dict 받음 
def getCountryNameDictFromIMF():
    response = requests.get("https://www.imf.org/external/datamapper/api/v1/countries") # IMF에서 국가 목록 API 리퀘스트
    countryNames = response.json()["countries"]
    with open("missions/W1/M3/Country_Names.json", "w") as f: # 국가 목록 json file로 저장
        f.write(response.text)
    return countryNames

# Transform
# 1. json 읽어 DF 생성
# Parameter: Json object(NGDPD Raw data)
# return: DataFrame(raw DataFrame)
def getDFFromJson(js):
    js_values = js.get("values")
    js_NGDPD = js_values.get("NGDPD")
    df = pd.DataFrame(js_NGDPD).T
    return df

# 2. 해당년도 GDP 컬럼 선택
# Parameter: DataFrame(raw DataFrame)
# return: DataFrame -> (국가코드 / 선택된 년도의 GDP 정보)
def getGDPOfYear(df, year):
    return df[str(year)].to_frame()

# 3. 2025열 "GDP_USD_billion"으로 이름 변경하여 반환
# Parameter: DataFrame(국가코드 / 2025)
# return: DataFrame(국가코드 / GDP_USD_billion)
def renameGDP(df):
    df.columns = ["GDP_USD_billion", ]
    return df

# 4. 결측값 제거
# Parameter: DataFrame(국가코드 / GDP_USD_billion)
# return: DataFrame(국가코드 / GDP_USD_billion) <- NaN값 있는 row 제거 됨
def dropIfNaN(df):
    return df.dropna(axis = 0)

# 5. GDP 소수 둘째 자리까지 반올림
# Parameter: DataFrame(국가코드 / GDP_USD_billion)
# return: DataFrame(국가코드 / GDP_USD_billion) GDP_USD_billion의 값 모두 소수 둘째 자리까지 반올림
def roundGDP(df):
    df["GDP_USD_billion"] = df["GDP_USD_billion"].apply(lambda x:round(x, 2))
    return df

# 6. GDP 순으로 정렬된 df 반환
# Parameter: DataFrame(국가코드 / GDP_USD_billion)
# return: sorted DataFrame(국가코드 / GDP_USD_billion) by GDP_USD_billion descending
def sortByGDP(df):
    return df.sort_values("GDP_USD_billion", ascending = False)

# 7. 국가가 아닌 row 제거
# Parameter: DataFrame(국가코드 / GDP_USD_billion), dict countryNames
# return: DataFrame(국가코드 / GDP_USD_billion) deleted row that is not country
def dropIfNotCountry(df, countryNames):
    return df[df.index.isin(countryNames)]

# 8. Country 열 추가
# Parameter: DataFrame(국가코드 / GDP_USD_billion), dict countryNames
# return: DataFrame(국가코드 / 국가명 + GDP_USD_billion)
def addCountryName(df, countryNames):
    countries = []
    for code in df.index: # index 순회 (국가 코드가 인덱스)
        countries.append(countryNames[code]["label"]) # 국명 추가
    df.insert(loc = 0,column = "Country", value = countries) # 국가명 열 삽입
    return df

# 9. pycountry_convert 패키지 이용하여 국가명 -> Region으로 변환
# Parameter: DataFrame(국가코드 / 국가명 + GDP_USD_billion)
# return: DataFrame(국가코드 / 국가명 + GDP_USD_billion + Region)
def addRegion(df):
    regions = []
    for code in df.index:   
        if code == "UVK":   # 예외처리, 코소보
            region = "Europe"
        elif code == "TLS": # 예외처리 동 티모르
            region = "Asia"
        else:
            alpha2 = pycountry_convert.country_alpha3_to_country_alpha2(code)   # 2글자 코드로
            regionCode = pycountry_convert.country_alpha2_to_continent_code(alpha2) # 지역 코드로
            region = pycountry_convert.convert_continent_code_to_continent_name(regionCode) # 지역명으로
        regions.append(region)
    df["Region"] = regions # Region 열 삽입
    return df

# 10. 인덱스 리셋하고 국가 코드 열 드랍
# Parameter: DataFrame(국가코드 / 국가명 + GDP_USD_billion + Region)
# return: DataFrame(국가명 + GDP_USD_billion + Region)
def resetIndex(df):
    return df.reset_index(drop = True)

# Load process
# SQL에 Table 로드
# Parameter: DataFrame (국가명 + GDP_USD_billion + Region)
# return: X
# Table Name: Countries_by_GDP
# Columns: Country TEXT
#          GDP_USD_billion REAL
#          Region TEXT
def load(df):
    path = "missions/W1/M3/World_Economies.db"
    con = sqlite3.connect(path)
    cursor = con.cursor()

# 1. SQL Query 직접 작성
    # 테이블 생성
    sql = """
    DROP TABLE IF EXISTS Countries_by_GDP;
    CREATE TABLE Countries_by_GDP
    (
    Country TEXT PRIMARY KEY,
    GDP_USD_billion REAL,
    Region TEXT
    );
    """
    cursor.executescript(sql)
    con.commit()
    
    # 레이블 삽입
    for row in df.itertuples(index = False):
        sql = "INSERT INTO Countries_by_GDP(Country, GDP_USD_billion, Region) values (?, ?, ?)"
        cursor.execute(sql, (row[0], row[1], row[2]))
    con.commit()

# 2. to_sql 메서드 사용
# 쿼리 직접 써서 주석 처리함
    # df.to_sql(name = "Countries_by_GDP", con = conn, if_exists = "replace", index = False,)
    # conn.commit()

    con.close()

# Result
# GDP가 100B 이상인 국가들 SQL query를 통해 출력
# Parameter: X
# return: X
def printCountriesOverGDP100B():
    path = "missions/W1/M3/World_Economies.db"

    if not os.path.isfile(path):    # 파일 검사
        print("DB 파일 없음")
        return
    
    # 이상 없으면 연결
    con = sqlite3.connect(path)
    cursor = con.cursor()
    sql = "SELECT Country, GDP_USD_billion FROM Countries_by_GDP WHERE GDP_USD_billion >= 100"
    cursor.execute(sql)
    rows = cursor.fetchall() # sql Select
    
    print("Country                     | GDP_USD_billion")
    for row in rows:
        country = "%-27s" % row[0]
        GDP = row[1]
        print(country + " |", GDP)
    print()

# Region별로 top5 국가의 GDP avg
# Parameter: X
# return: X
def printAvgOfTop5ByRegion():
    path = "missions/W1/M3/World_Economies.db"
    if not os.path.isfile(path):    # 파일 검사
        print("DB 파일 없음")
        return
    
    # 이상 없으면 연결
    con = sqlite3.connect(path)
    cursor = con.cursor()
    # Region Distinct
    sql = "SELECT DISTINCT Region FROM Countries_by_GDP"
    cursor.execute(sql)
    regions = cursor.fetchall() # SQL Select
    
    for region in regions:
        region = region[0]
        print(region + " average GDP of Top 5")
        sql = "SELECT avg(GDP_USD_Billion) FROM Countries_by_GDP WHERE Region = '" + region + "'"
        cursor.execute(sql)
        print(round(cursor.fetchone()[0], 2), "B")
    print()
    con.close()

# writeLog
def writeLog(log):
    now = dt.datetime.now()
    log = now.strftime("%Y-%b-%d-%H-%M-%S, ") + log + "\n" # Ex) 2025-Jan-06-18-04-01, Load start
    filePath = "missions/W1/M3/etl_project_log.txt"
    f = open(filePath, "a") # append
    f.write(log)
    f.close()

def main():
    # Extract
    writeLog("Extract start")
    jsGDP = getGDPJsonFromIMF()
    countryNames = getCountryNameDictFromIMF()
    writeLog("Extract finished")

    # Transform
    writeLog("Transform start")
    df = getDFFromJson(jsGDP) # 1
    df = getGDPOfYear(df, 2025) # 2
    df = renameGDP(df) # 3
    df = dropIfNaN(df) # 4
    df = roundGDP(df) # 5
    df = sortByGDP(df) # 6
    df = dropIfNotCountry(df, countryNames) # 7
    df = addCountryName(df, countryNames) # 8
    df = addRegion(df) # 9
    df = resetIndex(df) # 10
    writeLog("Transform finished")

    # Load
    writeLog("Load start")
    load(df)
    writeLog("Load finished")

    # Result
    printCountriesOverGDP100B()
    printAvgOfTop5ByRegion()

main()