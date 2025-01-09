from abc import ABC, abstractmethod
import pandas as pd
import sqlite3

from modules.logger import logger


class ExporterInterface(ABC):
    """
    Data exporter interface
    """

    def __init__(self, target: str, **options):
        self.target = target
        self.options = options

    @abstractmethod
    def export_data(self, df: pd.DataFrame):
        pass


class JsonFileExporter(ExporterInterface):
    """
    Json file data exporter
    """

    def export_data(self, df: pd.DataFrame):
        logger.info(f"Exporting data to {self.target}...")
        df.to_json(self.target, orient="records", indent=2)
        logger.info("Data exported successfully")


class SqliteExporter(ExporterInterface):
    """
    Sqlite data exporter
    """

    def __init__(self, target: str, table_name: str):
        if not table_name:
            logger.error("table_name is required for SqliteExporter")
            raise ValueError("table_name is required for SqliteExporter")
        super().__init__(target, table_name=table_name)

    # sqlite connection만 받아오는게 좋을까? -> 굳이? 전부 이 안으로 추상화하자.
    def export_data(self, df: pd.DataFrame):
        logger.info(f"Exporting data to {self.target}...")
        conn = sqlite3.connect(self.target)
        df.to_sql(self.options["table_name"], conn, if_exists="replace", index=False)
        conn.close()
        logger.info("Data exported successfully")
