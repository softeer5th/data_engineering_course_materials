import traceback
import pandas as pd

from missions.W1.M3.log.log import Logger

logger = Logger.get_logger()


def over100b_countries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Print countries with GDP over 100 billion dollars.
    """
    try:
        df = df.sort_values(by='GDP_USD_Billion', ascending=False)        
        return df[df['GDP_USD_Billion'] > 100]        
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'gdp가 1000억 달러 이상인 국가 출력 중 에러 발생: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None


def top5_gdp_avg_by_region(df: pd.DataFrame) -> pd.DataFrame:
    """
    Print top 5 countries by GDP and their average GDP by region.
    """
    try:
        # 각 그룹의 top 5 국가들과 값을 먼저 구한 후
        top_5_region_groupby = df.sort_values('GDP_USD_Billion', ascending=False).groupby('Region').head(5)
        countries_by_group = top_5_region_groupby.groupby('Region')['Country'].agg(list).reset_index()
        # 평균값 계산
        means_by_group = top_5_region_groupby.groupby('Region')['GDP_USD_Billion'].mean()

        # 결과 merge
        result = pd.merge(countries_by_group, means_by_group, on='Region')
        result = result.sort_values('GDP_USD_Billion', ascending=False)
        return result
        
    except Exception as e:
        full_err_msg = traceback.format_exc(chain=True)
        err_msg = full_err_msg.split('\n')[-2]
        logger.info(f'각 지역별 상위 5개 국가 및 평균 GDP 출력 중 에러 발생: {e}')
        logger.info(f'Full message: {full_err_msg}')
        logger.info(f'Short message: {err_msg}')
        return None        
