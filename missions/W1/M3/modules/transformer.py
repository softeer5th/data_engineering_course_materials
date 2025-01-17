import pandas as pd


def transform_gdp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert GDP to billion and sort by GDP.
    """

    df["GDP"] = (df["GDP"].str.replace(",", "").astype(float) / 1000).round(2)
    df = df.sort_values(by="GDP", ascending=False)
    df.rename(columns={"GDP": "GDP_USD_billion"}, inplace=True)

    return df
