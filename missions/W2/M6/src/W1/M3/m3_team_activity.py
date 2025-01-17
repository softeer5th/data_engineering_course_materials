import requests
import pandas as pd
import datetime as dt
import sqlite3
import json
from tabulate import tabulate
from enum import Enum

class Mode(Enum):
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'

state = Mode.EXTRACT
gdp_url = "https://www.imf.org/external/datamapper/api/v1/NGDPD?periods=2025,2025"
group_url = "https://www.imf.org/external/datamapper/api/v1/groups"
region_url = "https://www.imf.org/external/datamapper/api/v1/regions"

path = "./missions/W1/M3/data/"
db_name = "team_World_Economies.db"
region_data_name = "region.csv"
log_name = "team_etl_project_with_sql_log.txt"
gdp_json_name = "team_gdp.json"
region_json_name = "team_region.json"
group_json_name = "team_group.json"

def write_log(state: Mode, is_start: bool=True, is_error: bool=False, msg: Exception=None):
    with open(path + log_name, 'a') as log:
        time = dt.datetime.now().strftime('%Y-%b-%d-%H-%M-%S')
        if is_error:
            log.write(f'{time}, [{state.value}] Failed\n\t{msg}\n')
        elif is_start:
            log.write(f'{time}, [{state.value}] Started\n')
        else:
            log.write(f'{time}, [{state.value}] Ended\n')

def extract_data(url: str, f_name:str):
    try:
        ## API에서 데이터 가져오기
        data = requests.get(url)
        _save_to_json(f_name, data.text)
    except Exception as e:
        write_log(state, is_error=True, msg=e)
        exit(1)
    return data.text
    
def _save_to_json(f_name:str, data:str):
    # RAW data JSON 파일로 저장
    with open(path + f_name, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4)

def transform_data(gdp_data_str:str, group_data_str:str, region_data_str:str):
    ## json 객체로 변환
    gdp_json_obejct = json.loads(gdp_data_str)
    group_json_object = json.loads(group_data_str)
    region_json_object = json.loads(region_data_str)

    ## API에서 받은 JSON 데이터를 DataFrame으로 변환
    gdp_df = pd.DataFrame(gdp_json_obejct['values']['NGDPD'])
    group_df = pd.DataFrame(group_json_object['groups'])
    region_df = pd.DataFrame(region_json_object['regions'])

    ## DataFrame을 목적에 맞게 수정
    gdp_df = pd.melt(gdp_df)
    group_df = pd.melt(group_df)
    region_df = pd.melt(region_df)

    ## 국가가 아닌 데이터 제거
    gdp_df = gdp_df[~gdp_df['variable'].isin(group_df['variable'])]
    gdp_df = gdp_df[~gdp_df['variable'].isin(region_df['variable'])]

    gdp_df.rename(columns={'variable':'Country', 'value':'GDP_USD_billion'}, inplace=True)
    ## 국가별 대륙 매칭
    region_info_df = pd.read_csv(path + region_data_name)

    gdp_df = gdp_df.merge(
        region_info_df[['alpha-3','region']],
        left_on='Country',
        right_on='alpha-3',
        how='left'
        )
    gdp_df.drop(columns='alpha-3', inplace=True)
    gdp_df.sort_values(by='GDP_USD_billion',ascending=False, inplace=True)
    gdp_df['GDP_USD_billion'] = round(gdp_df['GDP_USD_billion'], 2)
    gdp_df.reset_index(inplace=True, drop=True)
    return gdp_df

def load_to_db(gdp_imf: pd.DataFrame):
    try:
        con = sqlite3.connect(path + db_name)
        gdp_imf.to_sql('Countries_by_GDP',con, if_exists='replace')
        con.close()
    except Exception as e:
        write_log(state, is_error=True, msg=e)
        exit(1)

def print_query_result(query:str):
    con = sqlite3.connect(path + db_name)
    cursor = con.cursor()
    data = cursor.execute(query).fetchall()
    con.close()
    columns = [description[0] for description in cursor.description]
    print(tabulate(data, headers=columns, tablefmt='grid', floatfmt='.2f'))

if __name__ == "__main__":
    # [EXTRACT]
    write_log(state, True)
    gdp_data_str = extract_data(gdp_url, gdp_json_name)
    group_data_str = extract_data(group_url, group_json_name)
    region_data_str = extract_data(region_url, region_json_name)
    write_log(state, False)
    state = Mode.TRANSFORM

    # [TRANSFORM]
    write_log(state, True)
    gdp_df = transform_data(gdp_data_str, group_data_str, region_data_str)
    write_log(state, False)
    state = Mode.LOAD

    # [LOAD]
    write_log(state, True)
    load_to_db(gdp_df)
    write_log(state, False)

    ## [Query를 사용해 출력하기]
    print_query_result(
        '''
        SELECT *
        FROM Countries_by_GDP
        WHERE GDP_USD_billion > 100;
        '''
        )

    print_query_result(
        '''
        WITH rankedByRegionGdp AS (
            SELECT
                Country,
                region,
                GDP_USD_billion,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY GDP_USD_billion DESC) AS rank
            FROM Countries_by_GDP
        )
        SELECT region, AVG(GDP_USD_billion)
        FROM rankedByRegionGdp
        WHERE rank <= 5
        GROUP BY region;
        '''
    )