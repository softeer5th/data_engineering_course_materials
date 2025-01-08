import json
from processor import extractor, loader, transformer
from utils.logging import Logger

# Wikipedia URL
WIKI_URL = (
    "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
)
# Log file path
LOG_FILE_PATH = "log/etl_project_log.txt"
# Extracted data path
EXTRACTED_DATA_PATH = "data/Countries_by_GDP.json"
# Processed data path
PROCESSED_DATA_PATH = "data/Countries_by_GDP_etl_processed.json"

if __name__ == "__main__":
    # Initialize logger
    logger = Logger(LOG_FILE_PATH)

    # Strart ETL process
    logger.info("======== Starting ETL Process =========")

    # Extract data
    logger.info("Extracting data...")
    extractor.extract(WIKI_URL, EXTRACTED_DATA_PATH)
    logger.info("Data extracted successfully.")

    # Transform data
    logger.info("Transforming data...")
    df = transformer.transform(EXTRACTED_DATA_PATH)
    logger.info("Data transformed successfully.")

    # Load data
    logger.info("Loading data...")
    loader.load(df, PROCESSED_DATA_PATH)
    logger.info("Data loaded successfully.")

    # End ETL process
    logger.info("======== ETL Process Completed ========")
