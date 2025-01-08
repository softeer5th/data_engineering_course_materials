from typing import List, Dict, Optional
import json

import sqlite3
from missions.W1.M3.log.log import Logger
from missions.W1.M3.utils.converter import (
    gdpstr2billions,
    country2region,
)

logger = Logger.get_logger()

def create_gdp_table(db_path: str, sql_path: str) -> Optional[bool]:
    """
    Create gdp table in the database bt SQL.

    Return True if the table is created successfully.
    """
    try:
        logger.info("데이터베이스 연결 시도")
        with sqlite3.connect(db_path)  as conn:
            cursor = conn.cursor()
            logger.info("데이터베이스 연결 성공")
            logger.info("sql문 읽기 시도")
            with open(sql_path, "r") as f:
                sql = f.read()
                logger.info("sql문 읽기 성공")
                logger.info("테이블 생성 시도")
                cursor.execute(sql)
                conn.commit()
        
        return True

    except Exception as e:
        logger.info(f'테이블 생성 시 에러 발생: {e}')
        return None

def load_gdp_json(json_path: str) -> Optional[List[Dict]]:
    """
    JSON file load.

    Return the loaded data.
    """
    try:
        logger.info("GDP JSON 로드 시도")
        with open(json_path, 'r') as json_file:
            gdp_data = json.load(json_file)
        return gdp_data
    
    except Exception as e:
        logger.info(f'GDP JSON 로드 중 에러 발생: {e}')
        return None
    
def transform_gdp_data(data: List[Dict]) -> Optional[List[Dict]]:
    """
    Transform the data.

    Return the transformed data.
    1. 'gdp' column is converted to float and divided by 1000 and rounded to 2 decimal places.
    2. 'year' column is converted to int.
    3. 'region' column is added based on the 'country' column. If the country is not in the country2region dictionary, 'Unknown' is added.

    """
    try:
        logger.info("데이터 변환 시도")
        transformed_data = []
        recent_row = None
        for row in data:
            recent_row = row
            transformed_data.append(
                {
                    'Country': row['country'],
                    'GDP_USD_billion': gdpstr2billions(row['gdp']),
                    'Year': int(row['year']) if row['year'] else None,
                    'Type': row['type'],
                    'Region': country2region.get(row['country'], "Unknown")
                }
            )
        return transformed_data
    
    except Exception as e:
        logger.info(f'데이터 변환 중 에러 발생: {e} / recent_row: {recent_row}')
        return None
            
def insert_one_gdp_data(db_path, transformed_data: List[Dict]) -> Optional[bool]:
    """
    Insert GDP data into DB
    """
    try:
        logger.info("데이터베이스 연결 시도")
        with sqlite3.connect(db_path)  as conn:
            cursor = conn.cursor()
            logger.info("데이터베이스 연결 성공")
            logger.info("데이터 삽입 시도")
            for row in transformed_data:
                cursor.execute(
                    '''
                    INSERT INTO gdp (Country, GDP_USD_billion, Year, Type, Region) 
                    VALUES (?, ?, ?, ?, ?)''',
                    (row['Country'], row['GDP_USD_billion'], row['Year'], row['Type'], row['Region']) # to prevent SQL injection
                )
            # 도중에 에러가 발생하지 않으면 commit
            conn.commit()
            return True
        
    except Exception as e:
        logger.info(f'데이터 삽입 중 에러 발생: {e}')
        return None
    
