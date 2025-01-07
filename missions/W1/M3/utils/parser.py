import requests
from bs4 import BeautifulSoup
from bs4.element import( 
    ResultSet, 
    Tag,
    )

from log.log import Logger
import json

logger = Logger.get_logger()


def fetch_wikipedia_page(url: str) -> BeautifulSoup:
    """
    Fetch from wikipedia page.
    """
    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        return soup

    except Exception as e:
        logger.info(f'에러 발생: {e}')
        return None

def get_gdp_table(soup: BeautifulSoup) -> Tag:
    try:
        tables = soup.find_all('table', class_=['wikitable', 'sortable'])

        gdp_table = None
        for table in tables:
            if table.caption and "GDP (million US$) by country" in table.caption.text:                
                gdp_table = table
                break

        if gdp_table is None:
            logger.info('테이블을 찾을 수 없습니다.') 
            raise ValueError("No table found")
        
        return gdp_table
    
    except Exception as e:
        logger.info(f'에러 발생: {e}')
        return None
    
def get_thead(table: Tag) -> Tag:
    try:
        th_values = table.find_all('th')
        for th in th_values:
            print(th.text)
        if th_values is None:
            logger.info('테이블 헤더를 찾을 수 없습니다.') 
            raise ValueError("No thead found")
        
        # 이후 추가 처리 필요...
        return th_values
    
    except Exception as e:
        logger.info(f'에러 발생: {e}')
        return None

def get_clean_tbody(table: Tag) -> Tag:
    try:
        tbody = table.tbody
        # tbody 내부에 th가 있음.
        if tbody is None:
            logger.info('테이블 바디를 찾을 수 없습니다.') 
            raise ValueError("No tbody found")
        
        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')
            # 빈 리스트가 아니고, 첫 번째 td가 "World"를 포함하고 있으면 삭제 후 break
            if tds and "World" in tr.find_all('td')[0]:
                tr.decompose()
                break
            tr.decompose()

        return tbody
    
    except Exception as e:
        logger.info(f'에러 발생: {e}')
        return None
    
def get_parsed_data(thead: Tag, tbody: Tag):
    """
    현재 thead 미사용.
    """
    try:
        parsed_data = []
        for tbody in tbody.find_all('tr'):
            td_values = tbody.find_all('td')
            
            for i in range(len(td_values)):
                sup_tags = td_values[i].find_all('sup')
                for sup in sup_tags:
                    sup.decompose()

            null_flag = False
            if td_values[1].attrs.get('class') and 'table-na' in td_values[1].attrs.get('class'):
                null_flag = True

            parsed_data.append({
                'country': td_values[0].text.strip(),
                'gdp': td_values[1].text.strip() if not null_flag else None,
                'year': td_values[2].text.strip() if not null_flag else None,
                'type': "IMF",
            })
        
        return parsed_data
        
    except Exception as e:
        logger.info(f'에러 발생: {e}')
        return None

def dump_json(data: list, file_path: str):
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.info(f'에러 발생: {e}')