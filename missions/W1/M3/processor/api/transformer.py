import multiprocessing as mp
import os
import re
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event

import pandas as pd

EXTRACTED_DATA_PATH = "data/api/extracted/gdp_%d.parquet"

YEAR_PATTERN = re.compile(f"\d{4}")

PROCESS_COUNT = mp.cpu_count()
CHUNK_SIZE = 5


def _read_parquet_chunk(
    file_path: str, offset: int, size: int
) -> pd.DataFrame:
    """
    Read a chunk of a Parquet file.
    :param file_path: str: File path.
    :param offset: int: Offset.
    :param size: int: Size.
    :return: pd.DataFrame: DataFrame.
    """
    return pd.read_parquet(file_path, offset=offset, rows=size)


def _read_country_chunk(
    data_path: str, year_range: range, offset: int, size: int
) -> pd.DataFrame:
    """
    Read a chunk of country data.
    :param data_path: str: Data path.
    :param year_range: range: Year range.
    :param offset: int: Offset.
    :param size: int: Size.
    :return: pd.DataFrame: DataFrame.
    """
    chunk_parts = []

    for year in year_range:
        df = _read_parquet_chunk(data_path % year, offset, size)
        df = df.set_index("country_code")
        df.columns = [year]
        chunk_parts.append(df)

    return pd.concat(chunk_parts, axis=1)


def _worker(
    data_path: str,
    year: int,
    year_range: range,
    row_cursor: Synchronized,
    result_queue: mp.Queue,
    done: Event,
) -> pd.DataFrame:
    while not done.is_set():
        with row_cursor.get_lock():
            row_start = row_cursor.value
            row_cursor.value += CHUNK_SIZE

        for row_index in range(row_start, row_start + CHUNK_SIZE):
            pass


def _get_year_range(data_path: str) -> range:
    data_dir = os.path.dirname(data_path)
    filenames = os.listdir(data_dir)

    years = []
    for filename in filenames:
        regex_match = YEAR_PATTERN.search(filename)
        if regex_match:
            years.append(int(regex_match.group()))

    return range(min(years), max(years) + 1)


def transform(data_path: str, year: int) -> pd.DataFrame:
    year_range = _get_year_range(data_path)

    row_cursor = mp.Value("i", row_cursor)
    result_queue = mp.Queue()
    done = mp.Event()

    processes = []
    for _ in range(PROCESS_COUNT):
        p = mp.Process(
            target=_worker,
            args=(data_path, year, year_range, row_cursor, result_queue, done),
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
