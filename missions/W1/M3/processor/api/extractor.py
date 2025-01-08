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
    df = pd.json_normalize(data)  # Flatten nested JSON, if necessary
    
    # Save to Parquet file
    df.to_parquet(f"{save_dir}/gdp_{year}.parquet", engine='pyarrow')