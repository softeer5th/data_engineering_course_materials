import glob
import os

from app.utils.logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from .constants import (
    ANALYZED_DATA_DIR,
    CLEANED_DATA_DIR,
    DATAFRAME_SCHEMA,
    SOURCE_DATA_DIR,
)


class Loader:
    def __init__(self, spark: SparkSession):
        self._logger = Logger("loader")
        self.spark = spark

    def load_source_list(self) -> list[DataFrame]:
        self._logger.info("Loading source data...")
        paths = os.path.join(SOURCE_DATA_DIR, "*.parquet")
        dfs = [self.spark.read.parquet(path) for path in glob.glob(paths)]
        self._logger.info(f"Source data loaded: {len(dfs)} files")
        return dfs

    def save_cleaned(self, df: DataFrame) -> None:
        self._logger.info("Cleaning data...")
        df.write.save(CLEANED_DATA_DIR, format="parquet", mode="overwrite")
        self._logger.info("Data cleaned and saved")

    def load_cleaned(self) -> DataFrame:
        self._logger.info("Loading cleaned data...")
        df = self.spark.read.parquet(CLEANED_DATA_DIR)
        self._logger.info("Cleaned data loaded")
        return df

    def save_analyzed(self, df: DataFrame) -> None:
        self._logger.info("Analyzing data...")
        df.write.save(ANALYZED_DATA_DIR, format="parquet", mode="overwrite")
        self._logger.info("Data analyzed and saved")

    def load_analyzed(self) -> DataFrame:
        self._logger.info("Loading analyzed data...")
        df = self.spark.read.parquet(ANALYZED_DATA_DIR)
        self._logger.info("Analyzed data loaded")
        return df
