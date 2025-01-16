import pandas as pd
import json
from utils import log_message
import os

RAW_DATA_FILE = 'results/Countries_by_GDP.json'
PROCESSED_DATA_FILE = 'results/Countries_by_GDP_Processed.json'
REGION_MAPPING_FILE = 'data/country_region_table.json'

def transform():
    df = pd.read_json(RAW_DATA_FILE)

    df = df[df['Country'] != 'World']
    df = df[df['GDP_USD_billion'] != '—']

    df['GDP_USD_billion'] = pd.to_numeric(df['GDP_USD_billion'].str.replace(',', ''), errors='coerce')
    df['GDP_USD_billion'] = (df['GDP_USD_billion'] / 1000).round(2)

    with open(REGION_MAPPING_FILE, 'r') as f:
        region_mapping = json.load(f)
    df['Region'] = df['Country'].map(region_mapping)

    df.to_json(PROCESSED_DATA_FILE, orient='records', indent=4)
    log_message(f"{len(df)}개 데이터 변환 완료 및 저장: {PROCESSED_DATA_FILE}")

if __name__ == '__main__':
    transform()