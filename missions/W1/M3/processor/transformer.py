import json
import numpy as np
import pandas as pd

# Country regions data path
COUNTY_REGIONS_DATA_PATH = "data/country_regions.json"


def _open_json_to_df(json_path: str) -> pd.DataFrame:
    """
    Open JSON file and convert it to DataFrame.
    :param json_path: str: JSON file path.
    :return: pd.DataFrame: DataFrame.
    """
    with open(json_path, "r") as file:
        data = json.load(file)

    df = pd.DataFrame.from_dict(data)
    return df


def _parse_gdp_strings(series: pd.Series) -> pd.Series:
    """
    Parse GDP strings to numeric values.
    :param series: pd.Series: Series with GDP strings.
    :return: pd.Series: Series with parsed GDP values.
    """
    return pd.to_numeric(
        series.astype(str).str.replace(",", ""), errors="coerce"
    )


def _parse_year_strings(series: pd.Series) -> pd.Series:
    """
    Parse year strings to numeric values.
    :param series: pd.Series: Series with year strings.
    :return: pd.Series: Series with parsed year values.
    """
    return pd.to_numeric(series, errors="coerce").astype("Int32")


def _add_country_regions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add regions to DataFrame based on country names.
    :param df: pd.DataFrame: DataFrame.
    :return: pd.DataFrame: DataFrame with regions.
    """
    with open(COUNTY_REGIONS_DATA_PATH, "r") as file:
        country_regions = json.load(file)

    df["region"] = df["country"].map(country_regions)
    return df


def transform(json_path) -> pd.DataFrame:
    """
    Transform extracted data.
    :param json_path: str: Extracted data JSON file path.
    :return: pd.DataFrame: Transformed DataFrame.
    """
    df = _open_json_to_df(json_path)

    df["gdp"] = _parse_gdp_strings(df["gdp"])
    df["gdp"] = (df["gdp"] / 1000).round(2)
    df["year"] = _parse_year_strings(df["year"])

    df = _add_country_regions(df)
    return df
