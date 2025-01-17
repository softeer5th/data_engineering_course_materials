import traceback
from typing import Optional, List
import sqlite3

from missions.W1.M3.log.log import Logger

logger = Logger.get_logger()

def sql_over100b_countries(db_path: str) -> Optional[List]:
    """
    Print countries with GDP over 100 billion dollars.
    """
    try:
        with sqlite3.connect(db_path)  as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT country FROM gdp
                WHERE GDP_USD_billion >= 100
                ORDER BY GDP_USD_billion DESC
                '''
                # IS NOT NULL을 하지 않아도 됨.
                )
            rows = cursor.fetchall()
            return rows
            
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'gdp가 1000억 달러 이상인 국가 출력 중 에러 발생: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None


def sql_top5_gdp_avg_by_region(db_path: str) -> Optional[List]:
    """
    Print top 5 countries by GDP and their average GDP by region.
    """
    try:
        with sqlite3.connect("missions/W1/M3/data/World_Economies.db")  as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT 
                    Region,
                    ROUND(avg(GDP_USD_billion), 2) as top_5_avg_GDP
                    /* 반올림을 안하면 파이썬 차원에서 또 다시 소숫점 둘째 자리 뒤까지 보여준다. */
                FROM (
                    SELECT
                        Country,
                        Region,
                        GDP_USD_billion,
                        ROW_NUMBER() OVER (
                            PARTITION BY Region
                            ORDER BY GDP_USD_billion DESC
                            ) as rank
                            FROM gdp
                        )
                WHERE rank <= 5
                GROUP BY Region
                ORDER BY top_5_avg_GDP DESC
                ;

                '''
                # IS NOT NULL을 하지 않아도 됨.
                )
            rows = cursor.fetchall()
            return rows
            logger.info(f"결과 수: {len(rows)}", )
            for row in rows:
                logger.info(row)
        
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'각 지역별 상위 5개 국가 및 평균 GDP 출력 중 에러 발생: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None        
