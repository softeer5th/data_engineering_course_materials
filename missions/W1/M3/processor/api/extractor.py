import pandas as pd
import requests
import multiprocessing as mp


def _extract(api_url: str, year: int, save_dir: str) -> None:
    """
    Extract data from API and save it to JSON file.
    :param api_url: str: API URL.
    :param year: int: Year.
    :param save_dir: str: Save directory.
    """
    response = requests.get(f"{api_url}?periods={year}")
    data = response.json()
    data = data["values"]["NGDPD"]

    df = pd.DataFrame(data).T
    df.index.name = "country_code"
    df = df.reset_index()

    # Save to Parquet file
    df.to_parquet(f"{save_dir}/gdp_{year}.parquet", engine="pyarrow")


def extract(api_url: str, start_year, save_dir: str) -> None:
    """
    Extract data from API for a range of years.
    :param api_url: str: API URL.
    :param year_range: range: Year range.
    :param save_dir: str: Save directory.
    """

    cur_year = mp.Value("i", start_year)
    stop_flag = mp.Event()

    with mp.Pool() as pool:
        pool.map(
            _extract,
            [api_url] * 6,
            range(start_year, start_year + 6),
            [save_dir] * 6,
        )
