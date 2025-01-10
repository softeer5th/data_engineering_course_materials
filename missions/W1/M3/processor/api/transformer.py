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


"""
인터페이스 정의
    transform
        특정 연도의 데이터를 가공하여 DataFrame 객체로 반환

        여러 _worker를 호출하여 결과를 종합한다.
            _worker 호출 시 parquet 파일들의 연도를 직접 확인해야 한다.
            data_path 내부의 parquet 파일들의 연도 범위를 구해야 한다.

내부 함수 정의
    _worker
        하나의 _worker는 k개의 국가에 대한 처리를 담당한다.
        물론 국가별로 따로 연산을 수행해야 한다.

    _process_country
        단일 국가에 대해 특정 year에 대한 연산을 수행한다.
            형 변환, 결측치 처리

    _read_country_chunk
        여러 parquet 파일에서 특정 범위의 행을 읽는다.
        어차피 여러 프로세스가 병렬적으로 실행되고 있으므로, I/O에 대한 추가적인 병렬 처리는 필요 없다.

    _read_parquet_chunk
        단일 parquet 파일에서 특정 범위의 행을 읽는다.
        
    _process_country
        단일 국가에 대해 특정 year에 대한 연산을 수행한다.
            형 변환, 결측치 처리
"""
