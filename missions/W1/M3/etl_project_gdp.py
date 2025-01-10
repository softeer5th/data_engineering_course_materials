import requests
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
import re
import datetime

LOG_DIR = "./etl_project_log.txt"

# urls
urlGDP = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
urlRegion = 'https://en.wikipedia.org/wiki/List_of_countries_and_territories_by_the_United_Nations_geoscheme'

# backup file name
bakupFile = {urlGDP:"wikipediaGDP", urlRegion:"wikipediaRegion"}

# decorator for logging
# this decorator helps to log the ETL processes.
# function name, its start and end time, and running time will be logged.
def withLog(func):
    def wrapper(*args, **kwargs):
        with open(LOG_DIR, "a") as f:
            f.write(datetime.datetime.now().strftime("%Y-%B-%d-%H-%M-%S,"))
            f.write(f"{func.__name__},start\n")
            startTime = datetime.datetime.now()

            result = func(*args, **kwargs)

            endTime = datetime.datetime.now()
            f.write(endTime.strftime("%Y-%B-%d-%H-%M-%S,"))
            f.write(f"{func.__name__},end,{endTime-startTime}{","+args[0] if func.__name__ == "extract" else ""}\n")
        return result
    return wrapper   


# get the gdp data from wikipedia
@withLog
def extract(url):
    # find past extract log
    logs = pd.read_csv(LOG_DIR, header=None, names=["time", "function", "status" ,"taken", "url"])
    lastAccess = logs[logs["url"] == url]
    
    # get http responce
    if lastAccess.empty:
        response = requests.get(url)
    else:
        lastAccess = datetime.datetime.strptime(lastAccess.iloc[-1]['time'],"%Y-%B-%d-%H-%M-%S") - datetime.timedelta(hours=9)
        response = requests.get(url, headers={"if-Modified-Since":lastAccess.strftime('%a, %d %b %Y %H:%M:%S GMT')})
    
    table = ""

    # check reponse
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        
        table = str(soup.select("table.wikitable")[0])
        
        # save the responce
        with open(bakupFile[url]+".bak", "w") as f:
            f.write(str(table))

    elif response.status_code == 304:
        # open from backup file
        try:
            with open(bakupFile[url]+".bak", "r") as f:
                table = f.read()
        except:
            print(response.status_code)        
    else : 
        print(response.status_code)

    return table

# transform the html table data to pandas data frame and process the data
@withLog
def transform(data):
    # html table to pandas data frame
    df = pd.read_html(StringIO(str(data)))[0]

    # delete annotation
    for col in df.columns:
        df[col] = df[col].apply(lambda x: re.sub(r'\[.*?\]', '', str(x)))
    
    return df

@withLog
def transformGDPTable(df):
    # drop column level and rename columns
    df = df.droplevel(level=0, axis=1)
    df.columns = ['Country/Territory', 'IMF Forecast', 'IMF Year', 'World Bank Estimate', 'World BankYear', 'UN Estimate',
       'UN Year']
    cols = df.columns

    # for year columns, transform the data to integer
    for col in cols[1:]: 
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.fillna(0)
    for col in cols[2::2]:
        df[col] = df[col].astype(int)
    # for GDP data, make billion unit and float
    for col in cols[1::2]:
        df[col] = round(df[col] / 1000.0, 2)
    df = df.drop(index = [0])
            
    return df

@withLog
def transformConjungrate(gdp, region):
    # left join with gdp and region.
    res = gdp.merge(
        region[['Country or Area', 'Geographical subregion']],
        left_on='Country/Territory',
        right_on='Country or Area',
        how='left'
    )

    # drop dupplicated column
    res = res.drop('Country or Area', axis=1)

    return res

# write the dataFrame to a json file
@withLog
def load(dataFrame):
    # extract dataFrame to json
    dataFrame.transpose().to_json('Countries_by_GDP.json')

def printOver100B(gdpData):
    print(gdpData[gdpData['IMF Forecast'] >= 100.0]['Country/Territory'])

def printTop5(gdpData):
    top5 = gdpData.groupby('Geographical subregion',).head(5)
    top5 = top5.groupby('Geographical subregion')['IMF Forecast'].mean()
    print(top5)

# main

# gdp ET process
gdp = extract(urlGDP)
gdp = transform(gdp)
gdp = transformGDPTable(gdp)

# region ET process
region = extract(urlRegion)
region = transform(region)

# merge two tables
gdpTable = transformConjungrate(gdp, region)

# and L, load merged table
load(gdpTable)

# print what we want
printOver100B(gdpTable)
printTop5(gdpTable)