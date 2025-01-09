import sqlite3
import time
import pandas as pd
from modules.logger import logger, init_logger
from modules.importer import CsvFileImporter
from modules.exporter import SqliteExporter

LOG_FILE_PATH = "etl_project_log.txt"
DB_PATH = "World_Economies.db"
TABLE_NAME = "Countries_by_GDP"
INPUT_FILE_PATH = "large_data_10M.csv"
# INPUT_FILE_PATH = "large_data.csv"

QUERY_1 = """
SELECT Country, GDP_USD_billion
FROM Countries_by_GDP
WHERE GDP_USD_billion > 100 
ORDER BY GDP_USD_billion DESC
"""
QUERY_2 = """
SELECT Region, AVG(GDP_USD_billion) FROM 
(
    SELECT
        Country,
        GDP_USD_billion,
        Region,
        ROW_NUMBER() OVER (PARTITION BY Region ORDER BY GDP_USD_billion DESC) AS row_num
    FROM Countries_by_GDP
)
WHERE row_num <= 5
GROUP BY Region
"""


def transfrom_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transformation function
    """
    time_start = time.time()
    # Million -> Billion
    # df["GDP"] = df["GDP"].apply(lambda x: x.replace(",", ""))
    # df["GDP"] = df["GDP"].apply(lambda x: round(float(x) / 1000, 2))

    # df["GDP"] = df["GDP"].apply(lambda x: round(float(x.replace(",", "")) / 1000, 2))
    # df["GDP"] = (df["GDP"].replace(",", "", regex=True).astype(float) / 1000).round(2)
    df["GDP"] = (df["GDP"].str.replace(",", "").astype(float) / 1000).round(2)
    # df["GDP"] = (
    #     pd.to_numeric(df["GDP"].str.replace(",", ""), errors="coerce")
    #     .div(1000)
    #     .round(2)
    # )
    time_end = time.time()
    logger.info(f"Transform GDP: {time_end - time_start:.2f} seconds")

    # Sort by GDP
    time_start = time.time()
    df = df.sort_values(by="GDP", ascending=False)
    time_end = time.time()
    logger.info(f"Sort by GDP: {time_end - time_start:.2f} seconds")

    # Rename GDP column to GDP_USD_billion
    df.rename(columns={"GDP": "GDP_USD_billion"}, inplace=True)

    return df


def main():
    init_logger(LOG_FILE_PATH)
    logger.print_separator()
    logger.info("Starting the ETL process")

    # Extract
    time_start = time.time()
    csv_importer = CsvFileImporter(INPUT_FILE_PATH)
    df = csv_importer.import_data()
    time_end = time.time()
    logger.info(f"Extract Time taken: {time_end - time_start:.2f} seconds")

    # Transform
    time_start = time.time()
    logger.info("Transforming data...")
    df = transfrom_df(df)
    time_end = time.time()
    logger.info(f"Transform Time taken: {time_end - time_start:.2f} seconds")

    print(df.head(3))

    # Load
    time_start = time.time()
    sqlite_exporter = SqliteExporter(DB_PATH, table_name=TABLE_NAME)
    sqlite_exporter.export_data(df)
    time_end = time.time()
    logger.info(f"Load Time taken: {time_end - time_start:.2f} seconds")

    logger.info("ETL process completed successfully")

    print("Top 5 Average GDP by Region:")

    time_start = time.time()
    df_groupby_top5 = df.groupby("Region").head(5)
    avg_gdp = df_groupby_top5.groupby("Region")["GDP_USD_billion"].mean()
    for region, gdp in avg_gdp.items():
        print(f"{region:<15} {gdp:.2f}")
    time_end = time.time()
    logger.info(f"Query with Dataframe : {time_end - time_start:.2f} seconds")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    time_start = time.time()
    cursor.execute(QUERY_2)
    for row in cursor:
        print(f"{row[0]:<15} {row[1]:.2f}")
    time_end = time.time()
    logger.info(f"Query with SQLITE: {time_end - time_start:.2f} seconds")
    conn.close()


if __name__ == "__main__":
    main()
