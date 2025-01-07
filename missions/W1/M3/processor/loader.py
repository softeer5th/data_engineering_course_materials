import json
import numpy as np
import pandas as pd

def _sort_by_gdp(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(by='gdp', ascending=False)

def _replace_nan_with_none(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace({np.nan: None})

def load(df: pd.DataFrame, json_path: str) -> None:
    df = _sort_by_gdp(df)
    df = _replace_nan_with_none(df)

    with open(json_path, 'w') as file:
        json.dump(df.to_dict(orient='index'), file, indent=2, ensure_ascii=False)