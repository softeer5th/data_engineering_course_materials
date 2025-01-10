from functools import partial
from multiprocessing import Pool

import pandas as pd

EXTRACTED_DATA_PATH = "data/api/extracted/gdp_%d.parquet"


def read_parquet_chunk(file_path: str, offset: int, size: int) -> pd.DataFrame:
    return pd.read_parquet(file_path, offset=offset, rows=size)


def read_country_chunk(
    data_path: str, year_range: range, offset: int, size: int
) -> pd.DataFrame:
    chunk_parts = []

    for year in year_range:
        df = read_parquet_chunk(data_path % year, offset, size)
        df = df.set_index("country_code")
        df.columns = [year]
        chunk_parts.append(df)

    return pd.concat(chunk_parts, axis=1)
