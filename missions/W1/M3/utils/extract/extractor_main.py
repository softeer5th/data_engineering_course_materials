from typing import Optional

from missions.W1.M3.utils.extract.parser import (
    fetch_wikipedia_page,
    get_gdp_table,
    get_thead,
    get_clean_tbody,
    get_parsed_data,
    dump_json,
    )
from missions.W1.M3.log.log import Logger

logger = Logger.get_logger("EXTRACT")

def extractor_main(gdp_html_url: str, dump_json_path: str) -> Optional[bool]:
    try:
        logger.info('데이터 추출 시작')
        # TODO: 직접 크롤링 대신 API 활용하기. 성능 개선 가능.
        soup = fetch_wikipedia_page(gdp_html_url)
        if soup is None:
            raise ValueError("No page found")
        
        logger.info("페이지 로드 및 soup 파싱 성공")

        gdp_table = get_gdp_table(soup)
        if gdp_table is None:
            raise ValueError("No table found")
        
        logger.info("테이블 로드 성공")
        
        # 데이터 변환
        logger.info('테이블 파싱 시작')

        thead = get_thead(gdp_table)
        if thead is None:
            raise ValueError("No thead found")
        
        logger.info("[현재 미사용!] 테이블 헤더 파싱 성공")

        tbody = get_clean_tbody(gdp_table)
        if tbody is None:
            raise ValueError("No tbody found")
        
        logger.info("테이블 바디 파싱 성공")        

        parsed_data = get_parsed_data(tbody)
        if parsed_data is None:
            raise ValueError("Parsing error!")
        
        logger.info("테이블 파싱 성공")
        
        # 데이터 JSON으로 저장
        logger.info("데이터 저장 시작")

        res = dump_json(parsed_data, dump_json_path)
        if res is None:
            raise ValueError("JSON 형식 저장 실패")
        
        logger.info('데이터 저장 완료')
        
        return True
    
    except Exception as e:
        logger.info(f'에러 발생: {e}')
        return None
