from pathlib import Path

HOME_DIR = Path(__file__).resolve().parent
LOG_FILE_PATH = HOME_DIR / "log/etl_project_log.txt"
RAW_DATA_FILE_PATH = HOME_DIR / "data/Countries_by_GDP.json"
OUTPUT_FILE_PATH = HOME_DIR / "data/Countries_by_GDP_Transformed.json"
DB_NAME = "World_Economies"
TABLE_NAME = "Countries_by_GDP"
DB_PATH = HOME_DIR / f"data/{DB_NAME}.db"
CSV_FILE_NAME = "large_data"
CSV_INPUT_FILE_PATH = HOME_DIR / f"data/{CSV_FILE_NAME}.csv"
