import json

import numpy as np
import pandas as pd

# Country regions data path
COUNTY_REGIONS_DATA_PATH = "data/country_regions.json"


def _parse_gdp_strings(series: pd.Series) -> pd.Series:
    """
    Parse GDP strings to numeric values.
    :param series: pd.Series: Series with GDP strings.
    :return: pd.Series: Series with parsed GDP values.
    """
    return pd.to_numeric(series.str.replace(",", ""), errors="coerce")


def _parse_year_strings(series: pd.Series) -> pd.Series:
    """
    Parse year strings to numeric values.
    :param series: pd.Series: Series with year strings.
    :return: pd.Series: Series with parsed year values.
    """
    return pd.to_numeric(series, errors="coerce").astype("Int16")


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


def _sort_by_gdp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort DataFrame by GDP in descending order.
    :param df: pd.DataFrame: DataFrame.
    :return: pd.DataFrame: Sorted DataFrame.
    """
    return df.sort_values(by="gdp", ascending=False)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform extracted data.
    :param df: pd.DataFrame: Extracted DataFrame.
    :return: pd.DataFrame: Transformed DataFrame.
    """
    df["gdp"] = _parse_gdp_strings(df["gdp"])
    df["gdp"] = (df["gdp"] / 1000).round(2)
    df["year"] = _parse_year_strings(df["year"])

    df = _add_country_regions(df)
    df = _sort_by_gdp(df)

    return df
