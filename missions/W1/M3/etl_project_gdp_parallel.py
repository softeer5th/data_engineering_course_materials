import sqlite3
import time
import pandas as pd
from pathlib import Path
from modules.logger import logger, init_logger, LogExecutionTime
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

HOME_DIR = Path(__file__).resolve().parent
LOG_FILE_PATH = HOME_DIR / "log/etl_project_log.txt"
DB_NAME = "World_Economies"
TABLE_NAME = "Countries_by_GDP"
INPUT_FILE_PATH = HOME_DIR / "data/large_data.csv"
DATA_SIZE = 10_000_000  # 10M rows
CHUNK_SIZE = 1_000_000  # 100K rows per chunk
NUM_CHUNKS = DATA_SIZE // CHUNK_SIZE  # 100 chunks

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

    df["GDP"] = (df["GDP"].str.replace(",", "").astype(float) / 1000).round(2)

    # Rename GDP column to GDP_USD_billion
    df.rename(columns={"GDP": "GDP_USD_billion"}, inplace=True)

    return df


schema = {
    "Country": str,
    "GDP": str,
    "Region": str,
}


def process_chunk(index: int):
    df = pd.read_csv(
        INPUT_FILE_PATH,
        dtype=schema,
        header=None,
        names=schema.keys(),
        skiprows=index * CHUNK_SIZE,
        nrows=CHUNK_SIZE,
    )
    df.to_csv(f"data/large_data_{index}.csv", index=False)


def process_chunk2(index: int, chunk: pd.DataFrame):
    print(f"Processing chunk {index}")
    chunk.to_csv(f"data/large_data_{index}.csv", index=False)


def extract_data_from_source():
    """
    (Extract) Seperate one big file into multiple small files.

    large_data_1B.csv -> large_data_1B_0.csv, large_data_1B_1.csv, ...
    """

    with Pool() as pool:
        with pd.read_csv(
            INPUT_FILE_PATH,
            dtype=schema,
            header=None,
            names=schema.keys(),
            chunksize=CHUNK_SIZE,
        ) as reader:
            results = []
            for i, chunk in enumerate(reader):
                results.append(pool.apply_async(process_chunk2, args=(i, chunk)))

        pool.close()
        pool.join()


def transform_chunk(index: int):
    """
    (Transform - Preprocess) Transform each small file
    """
    df = pd.read_csv(f"data/large_data_{index}.csv", dtype=schema)
    df = transfrom_df(df)
    df.to_csv(f"data/large_data_{index}_transformed.csv", index=False)


def map_by_region(index: int):
    """
    (Transform - Map) Separate each small file by region
    """
    df = pd.read_csv(f"data/large_data_{index}_transformed.csv")
    regions = ["Asia", "Europe", "Africa", "North America", "South America", "Oceania"]

    for region in regions:
        region_df = df[df["Region"] == region]
        region_df.to_csv(f"data/large_data_{index}_{region}.csv", index=False)


def reduce_by_region(region: str):
    """
    (Transform - Reduce) Merge all files for each region
    """
    all_files = [f"data/large_data_{i}_{region}.csv" for i in range(NUM_CHUNKS)]
    dfs = []

    for file in all_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
        except FileNotFoundError:
            continue

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv(f"data/large_data_{region}.csv", index=False)


def sort_by_gdp(region: str):
    """
    (Transform - Sort) Sort each region file by GDP
    """
    df = pd.read_csv(f"data/large_data_{region}.csv")
    df = df.sort_values(by="GDP_USD_billion", ascending=False)
    df.to_csv(f"data/large_data_{region}_sorted.csv", index=False)


def load_to_database(region: str):
    """
    (Load) Export each region file to sqlite
    """
    conn = sqlite3.connect(f"data/{DB_NAME}_{region}.db")
    df = pd.read_csv(f"data/large_data_{region}_sorted.csv")
    df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    conn.close()


def query_by_region(region: str):
    with sqlite3.connect(f"data/{DB_NAME}_{region}.db") as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT AVG(GDP_USD_billion) FROM (SELECT * FROM Countries_by_GDP ORDER BY GDP_USD_billion DESC LIMIT 5)"
        )
        result = cursor.fetchall()
        return region, result[0][0]


def main():
    init_logger(LOG_FILE_PATH)
    logger.print_separator()
    logger.info("Starting the Parallel ETL process")

    # 1. Extract
    with LogExecutionTime("Extract data"):
        # extract_data_from_source()
        with Pool() as pool:
            pool.map(process_chunk, range(NUM_CHUNKS))
        # with ThreadPoolExecutor() as executor:
        #     executor.map(process_chunk, range(NUM_CHUNKS))
        # with ProcessPoolExecutor() as executor:
        #     executor.map(process_chunk, range(NUM_CHUNKS))

    # 2. Transform - Preprocess
    with LogExecutionTime("Transform chunks"):
        with Pool() as pool:
            pool.map(transform_chunk, range(NUM_CHUNKS))

    # 3. Transform - Map
    with LogExecutionTime("Map by region"):
        with Pool() as pool:
            pool.map(map_by_region, range(NUM_CHUNKS))

    # 4. Transform - Reduce
    with LogExecutionTime("Reduce by region"):
        regions = [
            "Asia",
            "Europe",
            "Africa",
            "North America",
            "South America",
            "Oceania",
        ]
        with Pool() as pool:
            pool.map(reduce_by_region, regions)

    # 5. Sort
    with LogExecutionTime("Sort by GDP"):
        with Pool() as pool:
            pool.map(sort_by_gdp, regions)

    # 6. Load
    with LogExecutionTime("Load to database"):
        with Pool() as pool:
            pool.map(load_to_database, regions)

    # 7. Query
    regions = ["Asia", "Europe", "Africa", "North America", "South America", "Oceania"]
    with LogExecutionTime("Query by region"):
        with Pool() as pool:
            results = pool.map(query_by_region, regions)
            print(results)


if __name__ == "__main__":
    main()
