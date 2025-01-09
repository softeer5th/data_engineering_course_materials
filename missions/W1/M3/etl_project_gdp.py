from missions.W1.M3.utils.extract.extractor_main import extractor_main

from missions.W1.M3.log.log import Logger

logger = Logger.get_logger()

gdp_html_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
json_path = 'missions/W1/M3/data/Countries_by_GDP.json'

def etl_project_gdp():
    try:                
        res = extractor_main(gdp_html_url, json_path)
        if res is None:
            raise ValueError("Extract 실패")
        else:
            logger.info('Extract 성공')

    except Exception as e:
        logger.info(f'에러 발생: {e}')

    
if __name__ == '__main__':
    etl_project_gdp()