import sqlite3
from pathlib import Path
from modules.logger import logger, init_logger
from modules.importer import WikiWebImporter
from modules.exporter import SqliteExporter

HOME_DIR = Path(__file__).resolve().parent
LOG_FILE_PATH = HOME_DIR / "log/etl_project_log.txt"
DB_PATH = HOME_DIR / "data/World_Economies.db"
TABLE_NAME = "Countries_by_GDP"

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


def transfrom_df(df):
    """
    Transformation function
    """
    # Million -> Billion
    df["GDP"] = (df["GDP"].str.replace(",", "").astype(float) / 1000).round(2)

    # Sort by GDP
    df = df.sort_values(by="GDP", ascending=False)

    # Rename GDP column to GDP_USD_billion
    df.rename(columns={"GDP": "GDP_USD_billion"}, inplace=True)

    return df


def main():
    init_logger(LOG_FILE_PATH)
    logger.print_separator()
    logger.info("Starting the ETL process")

    # Extract
    wiki_importer = WikiWebImporter()
    df = wiki_importer.import_data()

    # Transform
    logger.info("Transforming data...")
    df = transfrom_df(df)

    # Load
    sqlite_exporter = SqliteExporter(DB_PATH, table_name=TABLE_NAME)
    sqlite_exporter.export_data(df)

    logger.info("ETL process completed successfully")

    # Query
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(QUERY_1)
    print("Countries with GDP > 100B:")
    for row in cursor:
        print(f"{row[0]:<20} {row[1]}")
    cursor.execute(QUERY_2)
    print("Top 5 Average GDP by Region:")
    for row in cursor:
        print(f"{row[0]:<15} {row[1]:.2f}")
    conn.close()


if __name__ == "__main__":
    main()
