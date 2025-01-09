import pandas as pd
import requests
import json

def extract(api_url: str, year: int, save_dir: str) -> None:
    """
    Extract data from API and save it to JSON file.
    :param api_url: str: API URL.
    :param year: int: Year.
    :param save_dir: str: Save directory.
    """
    response = requests.get(f"{api_url}?periods={year}")
    data = response.json()
    data = data['values']['NGDPD']

    df = pd.DataFrame(data).T
    df.index.name = 'country_code'
    df = df.reset_index()

    print(df.head(3))
    
    # Save to Parquet file
    df.to_parquet(f"{save_dir}/gdp_{year}.parquet", engine='pyarrow')