import requests
import json
from typing import Optional, List, Dict
import traceback

from bs4 import BeautifulSoup
from bs4.element import( 
    ResultSet, 
    Tag,
    )

from missions.W1.M3.log.log import Logger

logger = Logger.get_logger("parser")


def fetch_wikipedia_page(url: str) -> Optional[BeautifulSoup]:
    """
    Fetch html from wikipedia gdp page.

    Return BeautifulSoup object.
    """
    try:
        logger.info('페이지 요청 시작')
        r = requests.get(url)
        r.raise_for_status() # 로 대체 가능.
        logger.info('페이지 요청 완료')
        logger.info('페이지 파싱 시작')
        soup = BeautifulSoup(r.text, 'html.parser')
        logger.info('페이지 파싱 완료')
        return soup
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'위키피디아 페이지 로딩 오류: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None

def get_gdp_table(soup: BeautifulSoup) -> Optional[Tag]:
    """
    Get GDP table from wikipedia page.

    Return the first table that has caption "GDP (million US$) by country".
    """
    try:
        logger.info('테이블 찾기 시작')
        tables = soup.find_all('table', class_=['wikitable', 'sortable'])

        gdp_table = None
        for table in tables:
            if table.caption and "GDP (million US$) by country" in table.caption.text:                
                gdp_table = table
                break

        if gdp_table is None:
            logger.info('테이블을 찾을 수 없습니다.') 
            raise ValueError("No table found")
        
        logger.info('테이블 찾기 완료')
        return gdp_table
    
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'테이블 찾기 오류: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None
    
def get_thead(table: Tag) -> Optional[Tag]:
    """
    Get thead from the table.

    **[WARNING!] Not used in this project!**
    """
    try:
        logger.info('테이블 헤더 찾기 시작')
        th_values = table.find_all('th')
        for th in th_values:
            logger.info(th.text)
        if th_values is None:
            logger.info('테이블 헤더를 찾을 수 없습니다.') 
            raise ValueError("No thead found")
        
        logger.info('테이블 헤더 찾기 완료')
        # 이후 추가 처리 필요...
        return th_values
    
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'테이블 헤더 찾는 중 오류: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None

def get_clean_tbody(table: Tag) -> Optional[Tag]:
    """
    Get tbody from the table.

    Parse tbody and remove unnecessary rows.
    """

    try:
        logger.info('테이블 바디 찾기 시작')
        tbody = table.tbody
        # tbody 내부에 th가 있음.
        if tbody is None:
            logger.info('테이블 바디를 찾을 수 없습니다.') 
            raise ValueError("No tbody found")
        
        for tr in tbody.find_all('tr', class_=['static-row-header']): # static-row-header 개선.
            logger.info(f"tr 스킵 중... {tr.text.strip()}")
            tr.decompose()

        logger.info('테이블 바디 찾기 완료')
        return tbody
    
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'테이블 바디 찾는 중 오류: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None

def get_parsed_data(tbody: Tag, thead: Tag = None) -> Optional[List[Dict]]:
    """
    Parse tbody and return parsed data.

    [IMPORTANT] Only use IMF data in this project

    Return a list of dictionaries. 

    **[WARNING!] thead not used in this project!**   
    """
    try:
        logger.info('테이블 파싱 시작')
        parsed_data = []
        for tbody in tbody.find_all('tr'):
            td_values = tbody.find_all('td')
            
            for i in range(len(td_values)):
                sup_tags = td_values[i].find_all('sup')
                for sup in sup_tags:
                    logger.info(f"delete sup tag in Country: {td_values[0].text}")
                    sup.decompose()

            null_flag = False
            # table-na 클래스가 있으면 null data
            if td_values[1].attrs.get('class') and 'table-na' in td_values[1].attrs.get('class'):
                null_flag = True
                logger.info(f"null data in Country: {td_values[0].text}")

            parsed_data.append({
                'country': td_values[0].text.strip(),
                'gdp_usd_million': td_values[1].text.strip() if not null_flag else None,
                'year': td_values[2].text.strip() if not null_flag else None,
                'type': "IMF",
            })
        
        logger.info('테이블 파싱 완료')        
        return parsed_data
        
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'테이블 데이터 파싱 중 오류: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None

def dump_json(data: list, file_path: str) -> Optional[bool]:
    """
    dump data to json file.

    Return True if success.

    else return None.
    """
    try:
        logger.info(f'JSON으로 변환 시작 / 경로: {file_path}') # jsonl
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info('JSON으로 변환 완료')
        return True

    except Exception as e:
        # MARK: exception 클래스를 만들고, 파라메터로 메세지 넣기.
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'JSON 변환 중 오류: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None