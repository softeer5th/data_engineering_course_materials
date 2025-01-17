import pandas as pd
import sqlite3


def print_gdp_over_100_countries_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    (Pandas) Print countries with GDP > 100B.
    """
    df_over_100 = df[df["GDP_USD_billion"] > 100]
    print("Countries with GDP > 100B:")
    for _, row in df_over_100.iterrows():
        print(f"{row['Country']:<20} {row['GDP_USD_billion']}")


def print_gdp_over_100_countries_sql(db_path: str, table_name: str):
    """
    (SQLite) Print countries with GDP > 100B.
    """

    QUERY_1 = f"""
SELECT Country, GDP_USD_billion
FROM {table_name}
WHERE GDP_USD_billion > 100 
ORDER BY GDP_USD_billion DESC
"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(QUERY_1)
    print("Countries with GDP > 100B:")
    for row in cursor:
        print(f"{row[0]:<20} {row[1]}")
    conn.close()


def print_top5_avg_gdp_by_region_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Print top 5 average GDP by region.
    """
    df_groupby_top5 = df.groupby("Region").head(5)
    avg_gdp = df_groupby_top5.groupby("Region")["GDP_USD_billion"].mean()
    print("Top 5 Average GDP by Region:")
    for region, gdp in avg_gdp.items():
        print(f"{region:<15} {gdp:.2f}")


def print_top5_avg_gdp_by_region_sql(db_path: str, table_name: str):
    """
    (SQLite) Print top 5 average GDP by region.
    """

    QUERY_2 = f"""
SELECT Region, AVG(GDP_USD_billion) FROM 
(
    SELECT
        Country,
        GDP_USD_billion,
        Region,
        ROW_NUMBER() OVER (PARTITION BY Region ORDER BY GDP_USD_billion DESC) AS row_num
    FROM {table_name}
)
WHERE row_num <= 5
GROUP BY Region
"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(QUERY_2)
    print("Top 5 Average GDP by Region:")
    for row in cursor:
        print(f"{row[0]:<15} {row[1]:.2f}")
    conn.close()
