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

# decorator for logging
# this decorator helps to log the ETL processes.
# function name, its start and end time, and running time will be logged.
def withLog(func):
    def wrapper(*args, **kwargs):
        with open("./etl_project_log.txt", "a") as f:
            f.write(datetime.datetime.now().strftime("%Y-%B-%d-%H-%M-%S, "))
            f.write(f"{func.__name__} start\n")
            startTime = datetime.datetime.now()

            result = func(*args, **kwargs)

            endTime = datetime.datetime.now()
            f.write(endTime.strftime("%Y-%B-%d-%H-%M-%S, "))
            f.write(f"{func.__name__} end. in {endTime-startTime}\n")
        return result
    return wrapper   

# get the gdp data from wikipedia
@withLog
def extract_IMF_GDP(url):
    # get http response
    response = requests.get(url)

    # check reponse
    if response.status_code == 200:
        GDPJson= response.text
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
def load(dataFrame:pd.DataFrame, tableName:str, index:bool = False, index_label:str = ""):
    # extract dataFrame by jsontry:
    try:
        with sqlite3.connect(database) as conn:
            # Add table name 'gdp_data' and if_exists parameter
            dataFrame.to_sql(tableName, conn, if_exists='replace', index=index, index_label="Country")
            return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    
# main

# import gdp data from imf
url = 'https://www.imf.org/external/datamapper/api/v1/NGDPD'
imf_gdp = transform_IMF_GDP(extract_IMF_GDP(url))
load(imf_gdp, "imf_gdp", index=True, index_label="country")

# import iso countries' name data from saved csv file
ISO_Countries = pd.read_csv('./ISO_3166_Countries.csv')
load(ISO_Countries, "iso_country_name")

year = 2024

# print countries whose GDP is over 100B
sql = f"""SELECT DISTINCT i.name
          FROM imf_gdp g
          JOIN iso_country_name i ON i."alpha-3" = g.country
          WHERE g."{year}" >= 100.0;"""
print(*sendQuery(sql), sep="\n")

# print top 5 GDP mean of group by region
sql = f"""
        WITH ranked_countries AS (
            SELECT 
                g.country,
                i."sub-region",
                g."{year}" as gdp,
                RANK() OVER (PARTITION BY i."sub-region" ORDER BY g."{year}" DESC) as rank
            FROM imf_gdp g
            JOIN iso_country_name i ON i."alpha-3" = g.country
            WHERE g."{year}" IS NOT NULL
        )
        SELECT 
            "sub-region",
            COUNT(country) as country_count,
            ROUND(AVG(gdp), 2) as avg_gdp
        FROM ranked_countries
        WHERE rank <= 5
        GROUP BY "sub-region"
        ORDER BY avg_gdp DESC;
    """
print(*sendQuery(sql), sep="\n")

sql = f"""
        WITH ranked_countries AS (
            SELECT 
                g.country,
                i."sub-region",
                g."{year}" as gdp,
                RANK() OVER (PARTITION BY i."sub-region" ORDER BY g."{year}" DESC) as rank
            FROM imf_gdp g
            JOIN iso_country_name i ON i."alpha-3" = g.country
            WHERE g."{year}" IS NOT NULL
        )
        SELECT *
        FROM ranked_countries
        WHERE "sub-region" IS NULL
    """
print(*sendQuery(sql), sep="\n")