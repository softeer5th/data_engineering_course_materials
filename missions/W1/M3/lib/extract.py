import requests
from bs4 import BeautifulSoup
import json
from lib import log, constant

STATE = "EXTRACT"

def extract_table_from_web():
    log_full_name = constant.PATH + constant.LOG_NAME
    log.write_log(log_full_name, STATE, is_start=True)
    try:
        soup = _get_soup()
        _save_to_json(soup.text)
    except Exception as e:
        log.write_log(log_full_name, STATE, is_error=True, msg=e)
        exit(1)
    log.write_log(log_full_name, STATE, is_start=False)
    return soup

def _get_soup():
    response = requests.get(constant.URL)
    if response.status_code == 200:
        return BeautifulSoup(response.text, 'html.parser')
    
def _save_to_json(data:str):
    # RAW data JSON 파일로 저장
    with open(constant.PATH + constant.JSON_NAME, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4)