import json
import numpy as np
import pandas as pd

def _open_json_to_df(json_path: str) -> pd.DataFrame:
    with open(json_path, 'r') as file:
        data = json.load(file)

    df = pd.DataFrame.from_dict(data, orient='index')
    return df

def _parse_gdp_strings(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(',', ''), errors='coerce')

def _parse_year_strings(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors='coerce')

def transform(json_path) -> pd.DataFrame:
    df = _open_json_to_df(json_path)

    df['gdp'] = _parse_gdp_strings(df['gdp'])
    df['gdp'] = (df['gdp'] / 1000).round(2)
    df['year'] = _parse_year_strings(df['year'])

    return df