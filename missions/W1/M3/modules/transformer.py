import pandas as pd


def transform_gdp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert GDP to billion and sort by GDP.
    """

    df["GDP"] = (df["GDP"].str.replace(",", "").astype(float) / 1000).round(2)
    df = df.sort_values(by="GDP", ascending=False)

    return df


def rename_columns(df: pd.DataFrame, from_column: str, to_column: str) -> pd.DataFrame:
    """
    Rename columns.
    """
    df.rename(columns={from_column: to_column}, inplace=True)
    return df
