from typing import List, Dict, Optional

import sqlite3

from missions.W1.M3.utils.load.loader import (
    insert_one_gdp_data,
    create_gdp_table,
)
from missions.W1.M3.log.log import Logger

logger  = Logger.get_logger()

def loader_main(db_path: str, sql_path: str, transformed_data: List[Dict]) -> Optional[bool]:
    try:
        logger.info('데이터 로드 시작')
        logger.info("데이터베이스 연결 시도")
        with sqlite3.connect(db_path) as conn:
            logger.info("데이터베이스 연결 성공")
            
            # DB 테이블 생성
            res = create_gdp_table(conn, sql_path)
            if res is None:
                logger.info("DB 테이블 생성 실패")
                raise ValueError("테이블 생성 실패")
            else:
                logger.info("DB 테이블 생성 성공")
            
            # 데이터 삽입
            res = insert_one_gdp_data(conn, transformed_data)
            if res is None:
                raise ValueError("gdp 테이블에 데이터 삽입 실패")
            else:
                logger.info("gdp 테이블에 데이터 삽입 성공")
                return True
        
    except Exception as e:
        logger.info(f'LOAD 중 에러 발생: {e}')
        return None