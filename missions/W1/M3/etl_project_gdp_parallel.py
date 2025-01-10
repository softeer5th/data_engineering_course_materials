from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import sqlite3
import pandas as pd
from modules.logger import logger, init_logger, LogExecutionTime
from config import (
    LOG_FILE_PATH,
    DB_NAME,
    TABLE_NAME,
    CSV_FILE_NAME,
    CSV_INPUT_FILE_PATH,
)

DATA_SIZE = 10_000_000  # 10M rows
CHUNK_SIZE = 1_000_000  # 100K rows per chunk
NUM_CHUNKS = DATA_SIZE // CHUNK_SIZE  # 100 chunks
SCHEMA = {
    "Country": str,
    "GDP": str,
    "Region": str,
}


def transfrom_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transformation function
    """

    df["GDP"] = (df["GDP"].str.replace(",", "").astype(float) / 1000).round(2)

    df.rename(columns={"GDP": "GDP_USD_billion"}, inplace=True)

    return df


def read_and_save_chunk(index: int):
    df = pd.read_csv(
        CSV_INPUT_FILE_PATH,
        dtype=SCHEMA,
        header=None,
        names=SCHEMA.keys(),
        skiprows=index * CHUNK_SIZE,
        nrows=CHUNK_SIZE,
    )
    df.to_csv(f"data/{CSV_FILE_NAME}_{index}.csv", index=False)


def save_chunk(index: int, chunk: pd.DataFrame):
    print(f"Processing chunk {index}")
    chunk.to_csv(f"data/{CSV_FILE_NAME}_{index}.csv", index=False)


def sequential_split():
    """
    (Extract) Seperate one big file into multiple small files.

    large_data_1B.csv -> large_data_1B_0.csv, large_data_1B_1.csv, ...
    """

    with Pool() as pool:
        with pd.read_csv(
            CSV_INPUT_FILE_PATH,
            dtype=SCHEMA,
            header=None,
            names=SCHEMA.keys(),
            chunksize=CHUNK_SIZE,
        ) as reader:
            results = []
            for i, chunk in enumerate(reader):
                results.append(pool.apply_async(save_chunk, args=(i, chunk)))

        pool.close()
        pool.join()


def transform_chunk(index: int):
    """
    (Transform - Preprocess) Transform each small file
    """
    df = pd.read_csv(f"data/{CSV_FILE_NAME}_{index}.csv", dtype=SCHEMA)
    df = transfrom_df(df)
    df.to_csv(f"data/{CSV_FILE_NAME}_{index}_transformed.csv", index=False)


def map_by_region(index: int):
    """
    (Transform - Map) Separate each small file by region
    """
    df = pd.read_csv(f"data/{CSV_FILE_NAME}_{index}_transformed.csv")
    regions = ["Asia", "Europe", "Africa", "North America", "South America", "Oceania"]

    for region in regions:
        region_df = df[df["Region"] == region]
        region_df.to_csv(f"data/{CSV_FILE_NAME}_{index}_{region}.csv", index=False)


def reduce_by_region(region: str):
    """
    (Transform - Reduce) Merge all files for each region
    """
    all_files = [f"data/{CSV_FILE_NAME}_{i}_{region}.csv" for i in range(NUM_CHUNKS)]
    dfs = []

    for file in all_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
        except FileNotFoundError:
            continue

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv(f"data/{CSV_FILE_NAME}_{region}.csv", index=False)


def sort_by_gdp(region: str):
    """
    (Transform - Sort) Sort each region file by GDP
    """
    df = pd.read_csv(f"data/{CSV_FILE_NAME}_{region}.csv")
    df = df.sort_values(by="GDP_USD_billion", ascending=False)
    df.to_csv(f"data/{CSV_FILE_NAME}_{region}_sorted.csv", index=False)


def load_to_database(region: str):
    """
    (Load) Export each region file to sqlite
    """
    conn = sqlite3.connect(f"data/{DB_NAME}_{region}.db")
    df = pd.read_csv(f"data/{CSV_FILE_NAME}_{region}_sorted.csv")
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

    # 1. Split
    with LogExecutionTime("Split data"):
        # sequential_split()
        with Pool() as pool:
            pool.map(read_and_save_chunk, range(NUM_CHUNKS))
        # with ThreadPoolExecutor() as executor:
        #     executor.map(read_and_save_chunk, range(NUM_CHUNKS))
        # with ProcessPoolExecutor() as executor:
        #     executor.map(read_and_save_chunk, range(NUM_CHUNKS))

    # 2. Preprocess
    with LogExecutionTime("Transform chunks"):
        with Pool() as pool:
            pool.map(transform_chunk, range(NUM_CHUNKS))

    # 3. Map
    with LogExecutionTime("Map by region"):
        with Pool() as pool:
            pool.map(map_by_region, range(NUM_CHUNKS))

    # 4. Reduce
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
