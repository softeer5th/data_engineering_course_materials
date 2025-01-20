# 표준 라이브러리
import json

# 서드파티 라이브러리
import requests
import pandas as pd

# 로컬 모듈
from missions.W1.M3.log.log import Logger

logger = Logger.get_logger("GDP_ETL_LOGGER")

###########
#  1. Fetch
###########
def _fetch_web(url: str)-> str:
    """
    Fetches the content of a webpage from the specified URL.

    Args:
        url (str): The URL of the webpage to fetch.

    Returns:
        str: The HTML content of the fetched webpage as a string.

    Raises:
        HTTPError: If the HTTP request fails or the response status code is not 200.
    """ 
    try:
        logger.info(f"[START] Attempting to fetch the webpage: {url}")
        response = requests.get(url)
        response.raise_for_status()
        logger.info(f"[COMPLETE] Successfully fetched the webpage: {url}")
    except requests.exceptions.HTTPError as e:
        logger.error(f"[FAIL] Failed to fetch the webpage: {url}. Status Code: {response.status_code}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"[FAIL] Failed to fetch the webpage: {url}. Exception: {e.__class__.__name__}")
        raise
    return response.text

###########
#  2. Save to JSON
###########
def _response2json(data:str, key: str, url: str):
    try:
        logger.info(f"[START] Saving data to {url} Response ...")
        json_data = json.loads(data)
        json_data = json_data.get(key) if json_data.get(key) else json_data.get('values').get(key)
        logger.info(f"[COMPLETE] Data successfully are transformed to {url} Response ...")
    except (json.JSONDecodeError) as e:
        logger.error(f"[FAIL] Failed to transform data to {url} Response. Exception: {e.__class__.__name__}")
        
    return [{key: value} for key, value in json_data.items()]

def _save2jsonl(data_list: list, out_file: str)-> None:
    try:
        logger.info(f"[START] Saving data to {out_file} ...")
        with open(out_file, "a", encoding="utf-8") as jsonl_file:
            for data in data_list:
                json.dump(data, jsonl_file)
                jsonl_file.write('\n')
        logger.info(f"[COMPLETE] Data successfully saved to {out_file}.")
    except (OSError, json.JSONDecodeError) as e:
        logger.error(f"[FAIL] Failed to save data to {out_file}. Exception: {e.__class__.__name__}")
        raise

###########
#  Extract Process
###########
def extract_imf_api(url: str, key: str, param: str, out_file: str) -> None:
    try:
        logger.info("[START] Starting GDP extraction process...")

        raw_response = _fetch_web(url)
        json_data = _response2json(raw_response, key, url)
        _save2jsonl(json_data, out_file)
        logger.info("[COMPLETE] GDP extraction process completed successfully.")

        return raw_response
    except:
        logger.warn("[FAIL] GDP extraction process failed.")
        raise


###########
#  Transform Process
###########
def _transform_cleansing_duplicate(json_file: str):
    seen = set()

    # 중복 없는 데이터를 임시 저장할 리스트
    unique_lines = []

    # 중복 제거 과정
    with open(json_file, 'r', encoding='utf-8') as infile:
        for line in infile:
            line = line.strip()
            if not line:
                continue
            try:
                json_obj = json.loads(line)                      # JSON 객체로 변환
                json_str = json.dumps(json_obj, sort_keys=True)  # 중복 판별을 위해 정렬
            except json.JSONDecodeError:
                continue  # 잘못된 JSON은 건너뜀

            if json_str not in seen:
                seen.add(json_str)
                unique_lines.append(line)  # 중복이 아니면 리스트에 추가

    # 임시 파일 생성 후 덮어쓰기
    with open(json_file, 'w', encoding='utf-8') as outfile:
        outfile.write('\n'.join(unique_lines) + '\n')  # 모든 데이터를 한 번에 저장


def _transform_cleansing(json_file: str):
    _transform_cleansing_duplicate(json_file)

def transform_imf_api(json_file: str):
    _transform_cleansing(json_file)

def main():
    # param = 'countries'
    key = 'NGDPD'
    param = 'periods=2025'
    url_api_v1 = f"https://www.imf.org/external/datamapper/api/v1/{key}?{param}"
    out_file = f"missions/W1/M3/data/imf_{key}.jsonl"
    extract_imf_api(url_api_v1, key, param, out_file)
    transform_imf_api(out_file)

if __name__ == "__main__":
    main()