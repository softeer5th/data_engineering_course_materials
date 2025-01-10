import multiprocessing as mp
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event
from typing import Any

import pandas as pd
import requests

PROCESS_COUNT = mp.cpu_count() * 4


def _request_and_save(api_url: str, year: int, save_dir: str) -> bool:
    """
    Request GDP data from API and save to Parquet file.
    :param api_url: str: API URL.
    :param year: int: Year.
    :param save_dir: str: Save directory.
    :return: bool: True if successful, False otherwise.
    """
    response = requests.get(f"{api_url}?periods={year}")
    if response.status_code != 200:
        return False

    data = response.json()
    try:
        data = data["values"]["NGDPD"]
    except KeyError:
        return False

    df = pd.DataFrame(data).T
    df = df.reset_index()
    df.columns = ["code", "gdp"]

    # Save to Parquet file
    df.to_parquet(f"{save_dir}/gdp_{year}.parquet", engine="pyarrow")

    return True


def _worker(
    gdp_api_url: str, year_cursor: Synchronized, done: Event, save_dir: str
) -> None:
    """
    Worker function.
    :param api_url: str: API URL.
    :param year_cursor: Synchronized: Year cursor.
    :param done: Event: Done event.
    :param save_dir: str: Save directory.
    """
    while not done.is_set():
        with year_cursor.get_lock():
            year = year_cursor.value
            year_cursor.value += 1

        if not _request_and_save(gdp_api_url, year, save_dir):
            done.set()
            break


def extract(api_url: str, start_year: int, save_dir: str) -> None:
    """
    Extract GDP data from API.
    :param api_url: str: API URL.
    :param start_year: int: Start year.
    :param save_dir: str: Save directory.
    """

    year_cursor = mp.Value("i", start_year)
    done = mp.Event()

    processes = []
    for _ in range(PROCESS_COUNT):
        p = mp.Process(
            target=_worker, args=(api_url, year_cursor, done, save_dir)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
