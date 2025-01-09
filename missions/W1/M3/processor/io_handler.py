import json
import sqlite3

import pandas as pd


def save_dict_to_json(data: dict, json_path: str) -> None:
    """
    Save dictionary to JSON file.
    :param data: dict: Data.
    :param json_path: str: JSON file path.
    """
    with open(json_path, "w") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def save_dict_to_sqlite(data: dict, db_path: str, table_name: str) -> None:
    """
    Save dictionary to SQLite database.
    :param data: dict: Data.
    :param db_path: str: SQLite database path.
    :param table_name: str: Table name.
    """

    with sqlite3.connect(db_path) as conn:
        df = pd.DataFrame.from_dict(data)

        df = df.rename(
            columns={"country": "Country", "gdp": "GDP_USD_billion"}
        )

        df.to_sql(table_name, conn, if_exists="replace", index=False)


def open_json_as_df(json_path: str) -> pd.DataFrame:
    """
    Open JSON file and convert it to DataFrame.
    :param json_path: str: JSON file path.
    :return: pd.DataFrame: DataFrame.
    """
    with open(json_path, "r") as file:
        data = json.load(file)

    df = pd.DataFrame.from_dict(data)
    return df


def open_sqlite_as_df(db_path: str, table_name: str) -> pd.DataFrame:
    """
    Open SQLite database and convert it to DataFrame.
    :param db_path: str: SQLite database path.
    :param table_name: str: Table name.
    :return: pd.DataFrame: DataFrame.
    """

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

        df = df.rename(
            columns={"Country": "country", "GDP_USD_billion": "gdp"}
        )

    return df
