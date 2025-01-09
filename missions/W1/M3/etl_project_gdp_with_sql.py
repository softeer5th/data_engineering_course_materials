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

        if user_choice == 2 or user_choice == 3:
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
