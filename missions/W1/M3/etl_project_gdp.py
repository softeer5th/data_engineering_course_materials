import pandas as pd
from processor import extractor, io_handler, json_loader, transformer
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


def print_menu():
    """
    Print ETL process menu.
    """
    print("\n=== ETL Process Menu ===")
    print("1. Extract Data")
    print("2. Transform and Load Data")
    print("3. Run Full ETL Process")
    print("4. Exit")
    print("========================")


def get_user_choice():
    """
    Get user choice for ETL process.
    """
    while True:
        try:
            choice = int(input("\nEnter your choice (1-4): "))
            if 1 <= choice <= 4:
                print()
                return choice
            print("Invalid input. Please enter a number between 1 and 4.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def extract() -> bool:
    logger.info("Extracting data...")
    try:
        extracted_data = extractor.extract(WIKI_URL)
        io_handler.save_dict_to_json(extracted_data, EXTRACTED_DATA_PATH)
        logger.info("Data extracted successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        logger.info("======== ETL Process Aborted ========")
        return False
    return True


def transform() -> pd.DataFrame | None:
    logger.info("Transforming data...")
    try:
        df = io_handler.open_json_as_df(EXTRACTED_DATA_PATH)
        df = transformer.transform(df)
        logger.info("Data transformed successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data transformation: {e}")
        logger.info("======== ETL Process Aborted ========")
        return None
    return df


def load(df: pd.DataFrame) -> bool:
    logger.info("Loading data...")
    try:
        json_loader.load(df, PROCESSED_DATA_PATH)
        logger.info("Data loaded successfully.")
    except Exception as e:
        logger.error(f"Error occurred during data loading: {e}")
        logger.info("======== ETL Process Aborted ========")
        return False
    return True


if __name__ == "__main__":
    # Initialize logger
    logger = Logger(LOG_FILE_PATH)

    while True:
        # Print menu and get user choice
        print_menu()
        user_choice = get_user_choice()

        # Exit the program
        if user_choice == 4:
            print("Exiting the program...")
            break

        # Start ETL process
        logger.info("======== Starting ETL Process ========")

        if user_choice == 1 or user_choice == 3:
            # Extract data
            is_success = extract()
            if not is_success:
                continue
        if user_choice == 2 or user_choice == 3:
            # Transform data
            df = transform()
            if df is None:
                continue
            # Load data
            is_success = load(df)
            if not is_success:
                continue

        # End ETL process
        logger.info("======== ETL Process Completed ========")

    # 시각화는 ./etl_project_gdp_visualization.ipynb에서 진행하였습니다.
