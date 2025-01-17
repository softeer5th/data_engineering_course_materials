import pandas as pd
import sqlite3
from tabulate import tabulate

from lib import log, extract, transform, load, constant

def print_query_result(query:str):
    try:
        con = sqlite3.connect(constant.PATH + constant.DB_NAME)
        cursor = con.cursor()
        data = cursor.execute(query).fetchall()
        columns = [description[0] for description in cursor.description]
        con.close()
        print(tabulate(data, headers=columns, tablefmt='grid', floatfmt='.2f'))
    except Exception as e:
        log.write_log(constant.PATH, is_error=True, msg=e)
        exit(1)

if __name__ == "__main__":
    # [Extract]
    page = extract.extract_table_from_web()

    # [TRANSFORM]
    gdp_imf = transform.transform_table_to_data_frame(page)

    # [LOAD]
    load.load_to_db(gdp_imf)

    # [Query를 사용해 출력하기]
    print_query_result(
        '''
        SELECT *
        FROM Countries_by_GDP
        WHERE GDP_USD_billion > 100;
        '''
        )

    print_query_result(
        '''
        WITH rankedByRegionGdp AS (
            SELECT
                Country,
                region,
                GDP_USD_billion,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY GDP DESC) AS rank
            FROM Countries_by_GDP
        )
        SELECT region, AVG(GDP_USD_billion)
        FROM rankedByRegionGdp
        WHERE rank <= 5
        GROUP BY region;
        '''
    )
    