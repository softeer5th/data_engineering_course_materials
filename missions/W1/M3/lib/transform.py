from bs4.element import ResultSet, Tag
from bs4 import BeautifulSoup
import pandas as pd
from lib import log, constant

STATE = "TRANSFORM"

def transform_table_to_data_frame(page:BeautifulSoup) -> pd.DataFrame:
    log_full_name = constant.PATH + constant.LOG_NAME
    log.write_log(log_full_name, STATE, is_start=True)
    try:
        table = page.select_one('table.wikitable.sortable')
        head = table.select('tr.static-row-header')
        body = table.find_all('tr')

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
        log.write_log(log_full_name, STATE, is_error=True, msg=e)
        exit(1)

    log.write_log(log_full_name, STATE, is_start=False)
    return gdp_imf

# Table Parsing
def _parse_table_to_list(body: ResultSet[Tag]):
    region_df = pd.read_csv(constant.PATH + constant.REGION_NAME)
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