import requests
from bs4 import BeautifulSoup
from bs4.element import ResultSet, Tag
import pandas as pd
import datetime as dt
import json
from tabulate import tabulate
from enum import Enum

class Mode(Enum):
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'

state = Mode.EXTRACT
url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
path = "./missions/W1/M3/data/"
region_data_name = "region.csv"
log_name = "etl_project_log.txt"
json_name = "Countries_by_GDP.json"

def write_log(state: Mode, is_start: bool=True, is_error: bool=False, msg: Exception=None):
    with open(path + log_name, 'a') as log:
        time = dt.datetime.now().strftime('%Y-%b-%d-%H-%M-%S')
        if is_error:
            log.write(f'{time}, [{state.value}] Failed\n\t{msg}\n')
        elif is_start:
            log.write(f'{time}, [{state.value}] Started\n')
        else:
            log.write(f'{time}, [{state.value}] Ended\n')

def extract_table_from_web(url: str):
    try:
        soup = _get_soup(url)
        table = soup.select_one('table.wikitable.sortable')
        head = table.select('tr.static-row-header')
        body = table.find_all('tr')
        _save_to_json(path + json_name, table.text)
    except Exception as e:
        write_log(state, is_error=True, msg=e)
        exit(1)
    return head, body

def _get_soup(url:str):
    response = requests.get(url)
    if response.status_code == 200:
        return BeautifulSoup(response.text, 'html.parser')

def _save_to_json(f_name:str, data:str):
    # RAW data JSON 파일로 저장
    with open(f_name, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4)    

def transform_table_to_data_frame(head: ResultSet[Tag], body: ResultSet[Tag]) -> pd.DataFrame:
    try:
        column_list = _get_column_list(head)
        table_info = _parse_table_to_list(body)

        # 모든 기관의 정보가 담긴 DataFrame
        gdp_df = pd.DataFrame(table_info, columns=column_list)
        gdp_df['Year'] = gdp_df['Year'].astype('Int64')

        # IMF의 정보만 분리
        gdp_imf = gdp_df.iloc[:,:4]
        gdp_imf.rename(columns={'Forecast':'GDP', 'Country/Territory': 'Country'}, inplace=True)
        gdp_imf['GDP_USD_billion'] = round((gdp_imf['GDP'] / 1000), 2)
        gdp_imf.sort_values('GDP_USD_billion', ascending=False, inplace=True)
        gdp_imf.reset_index(drop=True, inplace=True)
    except Exception as e:
        write_log(state, is_error=True, msg=e)
        exit(1)
    return gdp_imf

# Table Parsing
def _parse_table_to_list(body: ResultSet[Tag]):
    region_df = pd.read_csv(path + region_data_name)
    table_info_list = []
    for rank, row in enumerate(body):
        if rank < 3: continue
        # 한 행의 정보를 담을 리스트
        row_info = []
        # 불필요한 정보를 제거
        while (row.sup != None):
            row.sup.decompose()
        # 정보 저장
        for idx, item in enumerate(row):
            value = item.text.strip()
            # 빈 셀 스킵
            if (value == ''): continue
            # 해당 기관의 정보가 없으면 예상치와 년도를 모두 NaN으로 설정
            elif (value == '—'): 
                row_info.append('NaN')
                row_info.append('NaN')
            # 정상 정보면 저장
            else: row_info.append(item.text.strip())
        # 문자열로 저장된 정보를 숫자로 변환
        for i in range(1, len(row_info)):
            row_info[i] = float(row_info[i].replace(',',''))
        # region 정보를 국가 이름과 매칭
        region = region_df[region_df['name'] == row_info[0]]['region'].values[0]
        row_info.insert(1, region)
        table_info_list.append(row_info)
    return table_info_list

# DataFrame 컬럼 리스트 생성
def _get_column_list(head: ResultSet[Tag]):
    category = head[0].find_all('th')[0].text.strip()
    temp_column = head[1].text.strip('\n').split('\n')
    column_list = [category, 'region'] + temp_column
    return column_list

def print_data_frame(df: pd.DataFrame):
    print(tabulate(df, headers='keys', tablefmt='grid'))

if __name__ == "__main__":

    # [Extract]
    write_log(state, True)
    head, body = extract_table_from_web(url)
    write_log(state, False)
    state = Mode.TRANSFORM

    # [TRANSFORM]
    write_log(state, True)
    gdp_imf = transform_table_to_data_frame(head, body)
    write_log(state, False)
    state = Mode.LOAD

    # [LOAD]
    write_log(state, True)
    write_log(state, False)

    # [SQL 사용하지 않고 접근하기]
    # GDP가 100B 이상 국가
    print_data_frame(gdp_imf[gdp_imf['GDP_USD_billion'] > 100])

    # 각 Region 별 상위 5개국 평균 GDP
    gdp_imf_grouped = gdp_imf.set_index(['region'])
    gdp_imf_grouped_top_5 = gdp_imf_grouped.sort_values(by=['region', 'GDP_USD_billion'], ascending=[True, False]).groupby('region').head(5)['GDP_USD_billion']
    gdp_imf_grouped_top_5_mean = gdp_imf_grouped_top_5.groupby(gdp_imf_grouped_top_5.index).mean()
    print(gdp_imf_grouped_top_5_mean)