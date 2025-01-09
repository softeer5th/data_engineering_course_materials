from processor.api import extractor, transformer
from utils.logging import Logger

# API URL
API_URL = (
    "https://www.imf.org/external/datamapper/api/v1/NGDPD"
)
# Log file path
LOG_FILE_PATH = "log/etl_project_log_using_api.txt"
# Extracted data directory
EXTRACTED_DATA_DIR = "data/api/extracted"
# Year range
YEAR_RANGE = range(2019, 2025)

if __name__ == "__main__":
    # Initialize logger
    logger = Logger(LOG_FILE_PATH)

    # Start ETL process
    logger.info("======== Starting ETL Process ========")

    # Extract data
    logger.info("Extracting data...")
    try:
        for year in YEAR_RANGE:
            extractor._extract(API_URL, year, EXTRACTED_DATA_DIR)
        logger.info("Data extracted successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        logger.info("======== ETL Process Aborted ========")
        exit()

    # Transform data
    # logger.info("Transforming data...")
    # try:
    #     transformer.analyze_gdp_data(EXTRACTED_DATA_DIR, 2024)
    #     logger.info("Data transformed successfully.")
    # except Exception as e:
    #     logger.error(f"Error occurred during data transformation: {e}")
    #     logger.info("======== ETL Process Aborted ========")
    #     exit()

    # End ETL process
    logger.info("======== ETL Process Completed ========")
