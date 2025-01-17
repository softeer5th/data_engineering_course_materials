from missions.W1.M3.utils.extract.extractor_main import extractor_main
from missions.W1.M3.utils.transform.transformer_main import transformer_main
from missions.W1.M3.utils.load.loader_main import loader_main
from missions.W1.M3.utils.analyze.analyzer_sql import (
        sql_over100b_countries,
        sql_top5_gdp_avg_by_region,
    )

from missions.W1.M3.log.log import Logger

logger = Logger.get_logger()

gdp_html_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
json_path = 'missions/W1/M3/data/Countries_by_GDP.json'
db_path = "missions/W1/M3/data/World_Economies.db"
sql_path = "missions/W1/M3/utils/sql/create_gdp.sql"

def etl_project_gdp_with_sql():
    try:                
        # res = extractor_main(gdp_html_url, json_path)
        # if res is None:
        #     raise ValueError("Extract 실패")
        
        # logger.info('Extract 성공')

        transformed_df = transformer_main(json_path)
        if transformed_df is None:
            raise ValueError("Transform 실패")
        
        logger.info('Transform 성공')

        res = loader_main(db_path, sql_path, transformed_df)
        if res is None:
            raise ValueError("Load 실패")
        
        logger.info('Load 성공')

        rows = sql_over100b_countries(db_path)
        if rows is None:
            raise ValueError("sql_over100b_countries 실패")
        else:
            logger.info(f"GDP가 100B USD이상이 되는 국가 수: {len(rows)}")
            for row in rows:
                logger.info(row)

        rows = sql_top5_gdp_avg_by_region(db_path)
        if rows is None:
            raise ValueError("sql_top5_gdp_avg_by_region 실패")
        else:
            logger.info("각 지역별 상위 5개 국가 평균 GDP")
            for row in rows:
                logger.info(row)


    except Exception as e:
        logger.info(f'에러 발생: {e}')

    
if __name__ == '__main__':
    etl_project_gdp_with_sql()