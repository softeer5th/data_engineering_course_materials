from typing import List, Dict, Optional

from missions.W1.M3.utils.transform.transformer import (
    transform_gdp_data,
    load_gdp_json,
    )
from missions.W1.M3.log.log import Logger

logger = Logger.get_logger("TRANSFORMER_MAIN")

# 데이터 변환
def transformer_main(load_json_path: str) -> Optional[List[Dict]]:
    try:
        gdp_data = load_gdp_json(load_json_path)
        if gdp_data is None:
            raise ValueError("GDP JSON 데이터 로드 실패")
        else:
            logger.info("GDP JSON 데이터 로드 성공")

        transformed_data = transform_gdp_data(gdp_data)
        if transformed_data is None:
            raise ValueError("데이터 변환 실패")
        else:
            logger.info("데이터 변환 성공")
            return transformed_data
        
    except Exception as e:
        logger.info(f'Transform 과정 중 에러 발생: {e}')
        return None