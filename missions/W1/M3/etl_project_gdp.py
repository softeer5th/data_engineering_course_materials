import requests
from bs4 import BeautifulSoup

from utils.parser import (
    fetch_wikipedia_page,
    get_gdp_table, 
    get_thead, 
    get_clean_tbody, 
    get_parsed_data, 
    dump_json,
    )

from log.log import Logger

logger = Logger.get_logger()


def etl_project_gdp():
    try:
        # 데이터 추출
        logger.info('데이터 추출 시작')

        soup = fetch_wikipedia_page("https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29")
        if soup is None:
            raise ValueError("No page found")

        gdp_table = get_gdp_table(soup)
        if gdp_table is None:
            raise ValueError("No table found")
        
        # 데이터 변환
        logger.info('데이터 변환 시작')

        thead = get_thead(gdp_table)
        if thead is None:
            raise ValueError("No thead found")

        tbody = get_clean_tbody(gdp_table)
        if tbody is None:
            raise ValueError("No tbody found")        

        parsed_data = get_parsed_data(thead, tbody)
        if parsed_data is None:
            raise ValueError("Parsing error!")
        
        # 데이터 변환
        logger.info('데이터 변환 종료')
        
        # 데이터 저장
        logger.info("데이터 저장 시작")
        dump_json(parsed_data, 'missions/W1/M3/data/Countries_by_GDP.json')
        logger.info('데이터 저장 완료')
        
    except Exception as e:
        logger.info(f'에러 발생: {e}')

if __name__ == '__main__':
    etl_project_gdp()
    