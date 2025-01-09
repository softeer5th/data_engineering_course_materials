import requests
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
import json
import datetime as dt
import pycountry
import pycountry_convert as pc

"""
Function Name : exctract
Type: Process
Description : extract Dataframe from given URL
Dependencies: loadSoup, findTable, saveDfToJson
Parameters : String url: url to extract raw data
Return Value : dataframe df : table of GDPs
Date Created : 2025/01/06
"""
def extract(url):
    writeLog("Extract start") # 시작 로그
    soup = loadSoup(url) # soup에 추출한 html 저장
    df = findTable(soup) # soup에서 목표한 테이블 찾아 DataFrame df로 저장
    saveDfToJson(df) # DataFrame df Json으로 저장
    writeLog("Extract finished") # 종료 로그
    return df

"""
Function Name : loadSoup
Description : load Soup object from given url
Parameters : String url: url for loading Soup
Return Value : BeautifulSoup soup: parsed html
Date Created : 2025/01/05
"""
def loadSoup(url):
    html = requests.get(url).text # html Text format으로 요청
    soup = BeautifulSoup(html, "html.parser") # soup 인스턴스 생성
    return soup

"""
# Function Name : findTable
# Description : find the GDP table from given BeautifulSoup object
# Parameters : BeautifulSoup soup: soup include the table
# Return Value : DataFrame df: table that have countries and GDP Data
# Date Created : 2025/01/05
"""
def findTable(soup):
    table = soup.find("table", class_ = "wikitable sortable sticky-header-multi static-row-numbers") # GDP Table 선택, find와 select 차이 공부해야겠다
    df = pd.read_html(StringIO(str(table)))[0] # pandas dataframe으로 리딩
    return df

"""
Function Name : saveDfToJson
Description : Save the DataFrame as a Json file before transform
Parameters : DataFrame df: raw table(before transform)
Return Value : Nothing(save the json file)
Date Created : 2025/01/06
"""
def saveDfToJson(df):
    filePath = "missions/W1/M3/Countries_by_GDP.json" # 파일경로 지정
    with open(filePath, "w") as f:
        json.dump(df.to_json(), f) # df json으로 변환 후 파일 쓰기

"""
Function Name: transform
Description: transform the raw DataFrame to the table we want
             transformed table info
                        Country      GDP
                1  United State 30337.16
                2         ...

Dependencies: filterTable, fillRegion
Parameters: DataFrame df (raw DataFrame)
Return Value: DataFrame df (Transformed DataFrame)
Date Created: 2025/01/06
"""
def transform(df):
    writeLog("Transform start") # 시작 로그

    df = filterTable(df) # DataFrame 정제(국가명 추출, 열 멀티인덱스 제거 및 정리, GDP "-"인 국가 0으로 변경, GDP열 float 타입으로 변환)
    df = fillRegion(df) # DataFrame Region column 생성

    writeLog("Transform finished") # 종료 로그
    return df

"""
Function Name : filterTable
Description : 
Parameters : String url: url for loading Soup
Return Value : BeautifulSoup soup <- parsed html
Date Created : 2025/01/05
"""
def filterTable(df):
    df = df.iloc[:,[0, 1]] # 국가명, GDP 추출
    df = df.droplevel(axis = 1, level = 0) # 열 멀티인덱스 제거
    df.columns = ["Country", "GDP"] # 컬럼명 변경
    for idx, row in enumerate(df["GDP"]): # GDP - 인 국가 0으로 변경
        if row == "—":
            df.loc[idx, "GDP"] = 0
    df = df.astype({"GDP":"float"}) # GDP열 float 타입으로 변환
    df["GDP"] = (df["GDP"] / 1000).round(2) #GDP열 단위 1M에서 1B로 변환
    df.drop(0, inplace=True) # 
    df.sort_values("GDP", ascending = False, inplace = True)
    return df

# fillRegion
def fillRegion(df):
    region = []
    for countryName in df["Country"]:
        region.append(getContinentFromCountry(countryName))
    df["Region"] = region
    return df

# getContinent
def getContinentFromCountry(countryName):
    if countryName == "DR Congo" or countryName == "Zanzibar":
        return "Africa"
    elif countryName == "Kosovo" or countryName == "Sint Maarten":
        return "Europe"
    elif countryName == "East Timor":
        return "Asia"
    countryAlpha2 = pc.country_name_to_country_alpha2(countryName)
    continentCode = pc.country_alpha2_to_continent_code(countryAlpha2)
    country_continent_name = pc.convert_continent_code_to_continent_name(continentCode)
    return country_continent_name

# Load
def load(df):
    writeLog("Load start")
    writeLog("Load finished")

def result(df):
    print(getCountriesOverGDP(df, 100))
    printAvgListByRegion(df)

# getCountriesOverGDP
def getCountriesOverGDP(df, Num):
    return df.loc[df.GDP >= Num]

def printAvgListByRegion(df):
    table = []
    regions = df["Region"].unique()
    for region in regions:
        print(region + ": ", getAvgOfTop5CountryFromRegion(df, region), "B")    

def getAvgList(df):
    table = []
    regions = df["Region"].unique()
    for region in regions:
        table.append([region,getAvgOfTop5CountryFromRegion(df, region)])
    return table

# getTop5CountryFromRegion
def getAvgOfTop5CountryFromRegion(df, region):
    top5 = df[df["Region"] == region].head(5)
    avg = top5["GDP"].mean().round(2)
    return avg

# writeLog
def writeLog(log):
    now = dt.datetime.now()
    log = now.strftime("%Y-%b-%d-%H-%M-%S, ") + log + "\n"
    filePath = "missions/W1/M3/etl_project_log.txt"
    f = open(filePath, "a")
    f.write(log)
    f.close()


# main
def main():
    # Extract
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)" # 국가별 GDP
    df = extract(url)

    # Transform
    df = transform(df)
    
    # Load
    load(df)

    # Result
    result(df)

main()