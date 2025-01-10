import requests
import pandas as pd
import json
import datetime
import sqlite3

# data base settings
database = "World_Economies.db"

# function to send a query
def sendQuery(sql):
    try:
        with sqlite3.connect(database) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            return cursor.fetchall()
        
    except sqlite3.Error as e:
        print(e)
        return []

LOG_DIR = "./etl_project_log.txt"

# urls
url = 'https://www.imf.org/external/datamapper/api/v1/NGDPD'

# backup file name
bakupFile = {url:"imfGDP"}

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
    
    # get response
    if lastAccess.empty:
        response = requests.get(url)
    else:
        lastAccess = datetime.datetime.strptime(lastAccess.iloc[-1]['time'],"%Y-%B-%d-%H-%M-%S") - datetime.timedelta(hours=9)
        response = requests.get(url, headers={"if-Modified-Since":lastAccess.strftime('%a, %d %b %Y %H:%M:%S GMT')})
    
    GDPJson = ""

    # check reponse
    if response.status_code == 200:
        GDPJson = response.text
        
        # save the response
        with open(bakupFile[url]+".bak", "w") as f:
            f.write(GDPJson)

    elif response.status_code == 304:
        # read saved response
        try:
            with open(bakupFile[url]+".bak", "r") as f:
                GDPJson = f.read()
        except:
            print(response.status_code) 
    else : 
        print(response.status_code)

    return GDPJson

# transform the html table data to pandas data frame and process the data
@withLog
def transform_IMF_GDP(data):
    # html table to pandas data frame
    df = json.loads(data)
    df = df['values']['NGDPD']
    df = pd.DataFrame(df).transpose()
    return df

# write the dataFrame to a json file
@withLog
def loadToSQL(dataFrame:pd.DataFrame, tableName:str, index:bool = False, index_label:str = ""):
    # extract dataFrame by jsontry:
    try:
        with sqlite3.connect(database) as conn:
            # Add table name 'gdp_data' and if_exists parameter
            dataFrame.to_sql(tableName, conn, if_exists='replace', index=index, index_label="Country")
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    
# print all information
def printAll(year, regionIdxSel):
    # print countries whose GDP is over 100B
    sql = f"""SELECT DISTINCT i.name
            FROM imf_gdp g
            JOIN iso_country_name i ON i."alpha-3" = g.country
            WHERE g."{year}" >= 100.0;"""
    print(f"\nCountries whose GDP is over 100B in {year}:")
    sqlResp = sendQuery(sql)
    for i in sqlResp:
        print(i[0])

    # print top 5 GDP mean of group by region
    sql = f"""
            WITH ranked_countries AS (
                SELECT 
                    g.country,
                    i."{regionIdxSel}",
                    g."{year}" as gdp,
                    RANK() OVER (PARTITION BY i."{regionIdxSel}" ORDER BY g."{year}" DESC) as rank
                FROM imf_gdp g
                JOIN iso_country_name i ON i."alpha-3" = g.country
                WHERE g."{year}" IS NOT NULL
            )
            SELECT 
                "{regionIdxSel}",
                COUNT(country) as country_count,
                ROUND(AVG(gdp), 2) as avg_gdp
            FROM ranked_countries
            WHERE rank <= 5
            GROUP BY "{regionIdxSel}"
            ORDER BY avg_gdp DESC;
        """
    print(f"\nAll {regionIdxSel}'s GDP means:")
    sqlResp = sendQuery(sql)
    for i in sqlResp:
        print(f"\t{i[0]}'s top{i[1]} GDP mean: {i[2]}")
    
    print("Unit: Billion USD")

# print a information in one region
def printOneRegion(year, regionIdxSel, regionSel):
    sql = f"""
        WITH ranked_countries AS (
            SELECT 
                g.country,
                i."{regionIdxSel}",
                g."{year}" as gdp,
                RANK() OVER (PARTITION BY i."{regionIdxSel}" ORDER BY g."{year}" DESC) as rank
            FROM imf_gdp g
            JOIN iso_country_name i ON i."alpha-3" = g.country
            WHERE g."{year}" IS NOT NULL
        )
        SELECT 
            COUNT(country) as country_count,
            ROUND(AVG(gdp), 2) as avg_gdp
        FROM ranked_countries
        WHERE "{regionIdxSel}" = "{regionSel}" AND rank <= 5
        ORDER BY avg_gdp DESC;
    """
    sqlResp = sendQuery(sql)
    print(f"top {sqlResp[0][0]} country's GDP mean equals {sqlResp[0][1]} in {year}")
    sql = f"""
        WITH ranked_countries AS (
            SELECT 
                g.country,
                i.name,
                i."{regionIdxSel}",
                g."{year}" as gdp,
                RANK() OVER (PARTITION BY i."{regionIdxSel}" ORDER BY g."{year}" DESC) as rank
            FROM imf_gdp g
            JOIN iso_country_name i ON i."alpha-3" = g.country
            WHERE g."{year}" IS NOT NULL
        )
        SELECT 
            name,
            gdp
        FROM ranked_countries
        WHERE "{regionIdxSel}" = "{regionSel}" AND rank <= 5
        ORDER BY gdp DESC;
    """
    sqlResp = sendQuery(sql)
    for data in sqlResp:
        print(f"\t{data[0]}: {data[1]}")

    print("Unit: Billion USD")

# main

# import gdp data from imf
url = 'https://www.imf.org/external/datamapper/api/v1/NGDPD'
imf_gdp = transform_IMF_GDP(extract(url))
loadToSQL(imf_gdp, "imf_gdp", index=True, index_label="country")

# import iso countries' name data from saved csv file
ISO_Countries = pd.read_csv('./ISO_3166_Countries.csv')
loadToSQL(ISO_Countries, "iso_country_name")

# user inputs
year = 2024
regionIdx = ["region","sub-region","intermediate-region"]
regionIdxSel = ""
regions = []
regionSel = ""
isSelectAll = False

# year select
while True:
    try:
        year = int(input("Target year:"))
    except:
        continue
    else:
        break

# region index select
for i, data in enumerate(regionIdx):
    print(f"{i+1}.", data)

while True:
    try:
        sel = int(input("your selection (1,2,3):"))
        regionIdxSel = regionIdx[sel - 1]
    except:
        continue
    else: 
        print(regionIdxSel, "is selected\n")
        break

# reion select
sql = f"""
    SELECT DISTINCT "{regionIdxSel}"
    FROM iso_country_name
"""
regions = sendQuery(sql)

for i, data in enumerate(regions):
    print(f"{i+1}.", data[0])

while True:
    try:
        sel = int(input("your selection (1,2,3..., 0 for all):"))
        if sel == 0:
            print("All is selected")
            isSelectAll = True
            break
        regionSel = regions[sel - 1][0]
    except:
        continue
    else: 
        print(regionSel, "is selected\n")
        break

# print information
if isSelectAll:
    printAll(year,regionIdxSel)

else:
    printOneRegion(year,regionIdxSel,regionSel)


