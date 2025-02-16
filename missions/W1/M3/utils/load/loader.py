from typing import List, Dict, Optional
import json
import traceback

import sqlite3
from missions.W1.M3.log.log import Logger

logger = Logger.get_logger()

def create_gdp_table(conn: sqlite3.Connection, sql_path: str) -> Optional[bool]:
    """
    Create gdp table in the database bt SQL.

    Return True if the table is created successfully.
    """
    try:
        cursor = conn.cursor()
        logger.info("sql문 읽기 시도")
        with open(sql_path, "r") as f:
            sql = f.read()
            logger.info("sql문 읽기 성공")
            logger.info("테이블 생성 시도")
            cursor.execute(sql)
            conn.commit()

        return True
    # TODO: 커스텀 예외 클래스를 만들어 코드 반복 줄일 수 있음.
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'테이블 생성 시 에러 발생: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None

def bulk_insert_gdp_data(conn: sqlite3.Connection, transformed_df) -> Optional[bool]:
    """
    Insert GDP data into DB
    """
    try:
        cursor = conn.cursor()
        logger.info("데이터 삽입 시도")
        cursor.executemany( # chunk 단위로도 나눌 수 있음. 도중에 에러가 발생하면 어느 row에서 발생한 것인지 파악하기 어렵기에 chunk 단위가 좋아보임.
            # 몇몇 DBMS는 executemany여도 도중에 에러가 발생하면 그 부분을 특정할 수 있음. 
            '''
            INSERT INTO gdp (Country, GDP_USD_billion, Year, Type, Region)
            VALUES (?, ?, ?, ?, ?)
            ''',
            [row for row in transformed_df.to_records(index=False).tolist()]
        )
        # 도중에 에러가 발생하지 않으면 한 번에 commit
        conn.commit()
        return True
        
    except Exception as e:
        conn.rollback()
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'데이터 삽입 중 에러 발생: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None