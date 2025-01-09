from missions.W1.M3.utils.extract.extractor_main import extractor_main
from missions.W1.M3.utils.transform.transformer_main import transformer_main
from missions.W1.M3.utils.load.loader_main import loader_main

from missions.W1.M3.log.log import Logger

logger = Logger.get_logger()

gdp_html_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
json_path = 'missions/W1/M3/data/Countries_by_GDP.json'
db_path = "missions/W1/M3/data/World_Economies.db"
sql_path = "missions/W1/M3/utils/sql/create_gdp.sql"

def etl_project_gdp_with_sql():
    try:                
        res = extractor_main(gdp_html_url, json_path)
        if res is None:
            raise ValueError("Extract 실패")
        else:
            logger.info('Extract 성공')

        transformed_data = transformer_main(json_path)
        if transformed_data is None:
            raise ValueError("Transform 실패")
        else:
            logger.info('Transform 성공')

        res = loader_main(db_path, sql_path, transformed_data)
        if res is None:
            raise ValueError("Load 실패")
        else:
            logger.info('Load 성공')

    except Exception as e:
        logger.info(f'에러 발생: {e}')

    
if __name__ == '__main__':
    etl_project_gdp_with_sql()