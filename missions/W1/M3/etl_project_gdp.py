from processor import extractor, transformer, io_handler, json_loader
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
    try:
        extracted_data = extractor.extract(WIKI_URL)
        io_handler.save_dict_to_json(extracted_data, EXTRACTED_DATA_PATH)
        logger.info("Data extracted successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        logger.info("======== ETL Process Aborted ========")
        exit()

    # Transform data
    logger.info("Transforming data...")
    try:
        df = io_handler.open_json_as_df(EXTRACTED_DATA_PATH)
        df = transformer.transform(df)
        logger.info("Data transformed successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data transformation: {e}")
        logger.info("======== ETL Process Aborted ========")
        exit()

    # Load data
    logger.info("Loading data...")
    try:
        json_loader.load(df, PROCESSED_DATA_PATH)
        logger.info("Data loaded successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data loading: {e}")
        logger.info("======== ETL Process Aborted ========")
        exit()

    # End ETL process
    logger.info("======== ETL Process Completed ========")

    # 시각화는 ./etl_project_gdp_visualization.ipynb에서 진행하였습니다.
