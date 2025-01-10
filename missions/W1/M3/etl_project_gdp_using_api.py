from processor.api import extractor, transformer
from utils.logging import Logger

# API URL
API_URL = "https://www.imf.org/external/datamapper/api/v1/NGDPD"
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
        extractor.extract(API_URL, 1980, EXTRACTED_DATA_DIR)
        logger.info("Data extracted successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        logger.info("======== ETL Process Aborted ========")
        raise e

    logger.info("======== ETL Process Completed ========")
