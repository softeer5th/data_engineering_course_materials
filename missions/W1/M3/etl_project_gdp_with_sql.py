from processor import extractor, transformer, io_handler, sqlite_loader
from utils.logging import Logger

# Wikipedia URL
WIKI_URL = (
    "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
)
# Log file path
LOG_FILE_PATH = "log/etl_project_log_etl_processed.txt"
# Database path
DATABASE_PATH = "data/World_Economies.db"
# Extracted data table name
EXTRACTED_DATA_TABLE = "Countries_by_GDP"
# Processed data table name
PROCESSED_DATA_TABLE = "Countries_by_GDP_etl_processed"

if __name__ == "__main__":
    # Initialize logger
    logger = Logger(LOG_FILE_PATH)

    # Strart ETL process
    logger.info("======== Starting ETL Process ========")

    # Extract data
    logger.info("Extracting data...")
    try:
        extracted_data = extractor.extract(WIKI_URL)
        io_handler.save_dict_to_sqlite(
            extracted_data, DATABASE_PATH, EXTRACTED_DATA_TABLE
        )
        logger.info("Data extracted successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        logger.info("======== ETL Process Aborted ========")
        exit()

    # Transform data
    logger.info("Transforming data...")
    try:
        df = io_handler.open_sqlite_as_df(DATABASE_PATH, EXTRACTED_DATA_TABLE)
        df = transformer.transform(df)
        logger.info("Data transformed successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data transformation: {e}")
        logger.info("======== ETL Process Aborted ========")
        exit()

    # Load data
    logger.info("Loading data...")
    try:
        sqlite_loader.load(df, DATABASE_PATH, PROCESSED_DATA_TABLE)
        logger.info("Data loaded successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data loading: {e}")
        logger.info("======== ETL Process Aborted ========")
        exit()

    # End ETL process
    logger.info("======== ETL Process Completed ========")

    # 시각화는 ./etl_project_gdp_with_sql_visualization.ipynb에서 진행하였습니다.
