from missions.W1.M3.utils.extract.extractor_main import extractor_main
from missions.W1.M3.utils.transform.transformer_main import transformer_main
from missions.W1.M3.utils.analyze.analyzer import (
    over100b_countries,
    top5_gdp_avg_by_region,
    )

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

        gdp_df = transformer_main(json_path)
        if gdp_df is None:
            raise ValueError("Transform 실패")
        else:
            logger.info('Transform 성공')

        # 분석
        logger.info('분석 시작')
        over100b_df = over100b_countries(gdp_df)
        if over100b_df is None:
            raise ValueError("over100b_countries 실패")
        else:
            logger.info("GDP가 1000억 달러 이상인 국가")
            countries_str = ", ".join(over100b_df['Country'].values)
            logger.info(countries_str)

        top5_gdp_avg_region_df = top5_gdp_avg_by_region(gdp_df)
        if top5_gdp_avg_region_df is None:
            raise ValueError("top5_gdp_avg_by_region 실패")
        else:
            logger.info("각 지역별 상위 5개 국가 및 평균 GDP")
            logger.info(top5_gdp_avg_region_df)
        
        logger.info('분석 완료')

    except Exception as e:
        logger.info(f'에러 발생: {e}')

    
if __name__ == '__main__':
    etl_project_gdp()