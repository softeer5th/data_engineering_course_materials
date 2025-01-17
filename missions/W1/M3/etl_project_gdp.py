from config import LOG_FILE_PATH, RAW_DATA_FILE_PATH, OUTPUT_FILE_PATH
from modules.logger import logger, init_logger
from modules.importer import WikiWebImporter
from modules.transformer import transform_gdp
from modules.exporter import JsonFileExporter
from modules.query_helper import (
    print_gdp_over_100_countries_df,
    print_top5_avg_gdp_by_region_df,
)


def main():
    init_logger(LOG_FILE_PATH)
    logger.print_separator()
    logger.info("Starting the ETL process")

    importer = WikiWebImporter(raw_data_file_path=RAW_DATA_FILE_PATH)
    df = importer.import_data()

    df = transform_gdp(df)

    exporter = JsonFileExporter(OUTPUT_FILE_PATH)
    exporter.export_data(df)

    logger.info("ETL process completed successfully")

    print_gdp_over_100_countries_df(df)
    print_top5_avg_gdp_by_region_df(df)


if __name__ == "__main__":
    main()
