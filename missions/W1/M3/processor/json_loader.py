import json
import numpy as np
import pandas as pd


def _replace_nan_with_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NaN values with None.
    :param df: pd.DataFrame: DataFrame.
    :return: pd.DataFrame: DataFrame with None values.
    """
    return df.replace({np.nan: None})


def load(df: pd.DataFrame, data_path: str) -> None:
    """
    Load DataFrame to JSON file.
    :param df: pd.DataFrame: DataFrame.
    :param data_path: str: JSON file path.
    """

    df = _replace_nan_with_none(df)

    with open(data_path, "w") as file:
        json.dump(
            df.to_dict(orient="records"), file, indent=2, ensure_ascii=False
        )
