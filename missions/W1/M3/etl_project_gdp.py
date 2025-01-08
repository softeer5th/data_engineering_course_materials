from missions.W1.M3.utils.parser import (
    fetch_wikipedia_page,
    get_gdp_table, 
    get_thead, 
    get_clean_tbody, 
    get_parsed_data, 
    dump_json,
    )

from missions.W1.M3.log.log import Logger

logger = Logger.get_logger()


def etl_project_gdp():
    try:
        logger.info('데이터 추출 시작')

        soup = fetch_wikipedia_page("https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29")
        if soup is None:
            raise ValueError("No page found")
        else:
            logger.info("페이지 로드 및 soup 파싱 성공")

        gdp_table = get_gdp_table(soup)
        if gdp_table is None:
            raise ValueError("No table found")
        else:
            logger.info("테이블 로드 성공")
        
        # 데이터 변환
        logger.info('테이블 파싱 시작')

        thead = get_thead(gdp_table)
        if thead is None:
            raise ValueError("No thead found")
        else:
            logger.info("[현재 미사용!] 테이블 헤더 파싱 성공")

        tbody = get_clean_tbody(gdp_table)
        if tbody is None:
            raise ValueError("No tbody found")
        else:
            logger.info("테이블 바디 파싱 성공")        

        parsed_data = get_parsed_data(tbody)
        if parsed_data is None:
            raise ValueError("Parsing error!")
        else:
            logger.info("테이블 파싱 성공")
        
        # 데이터 JSON으로 저장
        logger.info("데이터 저장 시작")
        res = dump_json(parsed_data, 'missions/W1/M3/data/Countries_by_GDP.json')
        if res is None:
            raise ValueError("JSON 형식 저장 실패")
        else:
            logger.info('데이터 저장 완료')
    except Exception as e:
        logger.info(f'에러 발생: {e}')

if __name__ == '__main__':
    etl_project_gdp()
    