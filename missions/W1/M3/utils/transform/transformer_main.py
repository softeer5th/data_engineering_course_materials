from typing import List, Dict, Optional

from pandas import DataFrame

from missions.W1.M3.utils.transform.transformer import (
    load_df_from_json,
    transform_gdp_df,
    )
from missions.W1.M3.log.log import Logger

logger = Logger.get_logger("TRANSFORMER_MAIN")

# 데이터 변환 (성능 개선 여지 충분)
def transformer_main(load_json_path: str) -> Optional[DataFrame]:
    try:
        # gdp_data = load_gdp_json(load_json_path)
        gdp_df = load_df_from_json(load_json_path)
        if gdp_df is None:
            raise ValueError("GDP JSON 데이터 로드 실패")
        else:
            logger.info("GDP JSON 데이터 로드 성공")

        # [해결]: 데이터 속성 값을 변환하는 것은 판다스가 더 효율적일 수 있음.
        # TODO: LOAD를 여러 장소에 하는 경우, 그리고 속성 값이 서로 다른 경우에는 속성값 변환이 LOAD 작업으로 편입될 수 있음.
        transformed_df = transform_gdp_df(gdp_df)
        if transformed_df is None:
            raise ValueError("데이터 변환 실패")
        else:
            logger.info("데이터 변환 성공")
            return transformed_df
        
    except Exception as e:
        logger.info(f'Transform 과정 중 에러 발생: {e}')
        return None