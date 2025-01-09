import pandas as pd
import sqlite3


def load(df: pd.DataFrame, db_path: str, table_name: str) -> None:
    """
    Load DataFrame to SQLite database.
    :param df: pd.DataFrame: DataFrame.
    :param db_path: str: SQLite database path.
    :param table_name: str: Table name.
    """
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
