from modules.logger import logger, init_logger
from modules.importer import WikiWebImporter
from modules.exporter import JsonFileExporter
from pathlib import Path

HOME_DIR = Path(__file__).resolve().parent
LOG_FILE_PATH = HOME_DIR / "log/etl_project_log.txt"
RAW_DATA_FILE_PATH = HOME_DIR / "data/Countries_by_GDP.json"
OUTPUT_FILE_PATH = HOME_DIR / "data/Countries_by_GDP_Transformed.json"


def transform_df(df):
    """
    Transformation function
    """
    # Million -> Billion
    df["GDP"] = (df["GDP"].str.replace(",", "").astype(float) / 1000).round(2)

    # Sort by GDP
    df = df.sort_values(by="GDP", ascending=False)

    return df


def main():
    init_logger(LOG_FILE_PATH)
    logger.print_separator()
    logger.info("Starting the ETL process")

    # Extract
    # parsing html and store to raw_data_file_path
    wiki_importer = WikiWebImporter(raw_data_file_path=RAW_DATA_FILE_PATH)
    df = wiki_importer.import_data()

    # Transform
    # transform GDP to billion and sort by GDP
    logger.info("Transforming data...")
    df = transform_df(df)

    # Load
    # export to output_file_path
    exporter = JsonFileExporter(OUTPUT_FILE_PATH)
    exporter.export_data(df)

    logger.info("ETL process completed successfully")

    # Query
    df_over_100 = df[df["GDP"] > 100]
    print("Countries with GDP > 100B:")
    for _, row in df_over_100.iterrows():
        print(f"{row['Country']:<20} {row['GDP']}")

    df_groupby_top5 = df.groupby("Region").head(5)
    avg_gdp = df_groupby_top5.groupby("Region")["GDP"].mean()
    print("Top 5 Average GDP by Region:")
    for region, gdp in avg_gdp.items():
        print(f"{region:<15} {gdp:.2f}")


if __name__ == "__main__":
    main()
