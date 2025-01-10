from config import LOG_FILE_PATH, DB_PATH, TABLE_NAME
from modules.logger import logger, init_logger
from modules.importer import WikiWebImporter
from modules.transformer import transform_gdp, rename_columns
from modules.exporter import SqliteExporter
from modules.query_helper import (
    print_gdp_over_100_countries_sql,
    print_top5_avg_gdp_by_region_sql,
)


def main():
    init_logger(LOG_FILE_PATH)
    logger.print_separator()
    logger.info("Starting the ETL process")

    importer = WikiWebImporter()
    df = importer.import_data()

    df = transform_gdp(df)
    df = rename_columns(df, "GDP", "GDP_USD_billion")

    exporter = SqliteExporter(DB_PATH, table_name=TABLE_NAME)
    exporter.export_data(df)

    logger.info("ETL process completed successfully")

    print_gdp_over_100_countries_sql(DB_PATH, TABLE_NAME)
    print_top5_avg_gdp_by_region_sql(DB_PATH, TABLE_NAME)


if __name__ == "__main__":
    main()
