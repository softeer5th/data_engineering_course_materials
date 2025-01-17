from abc import ABC, abstractmethod
import pandas as pd
import json
from bs4 import BeautifulSoup
import requests
from pathlib import Path
from modules.logger import logger


class ImporterInterface(ABC):
    """
    General Data importer interface.
    Importer Rule: import data from source and parse it to dataframe.

    The dataframe should have the following columns:
    - Country
    - GDP
    - Region
    """

    def __init__(self, source: str):
        self.source = source

    @abstractmethod
    def import_data(self) -> pd.DataFrame:
        """
        Import raw data from the source and parse it to dataframe.
        """
        pass


class WebImporterInterface(ImporterInterface):
    """
    Web Crawler interface. request(extract) -> parse(transform) -> return
    Subclass should implement _parse_html method.
    """

    def __init__(self, source: str, raw_data_file_path: str = None):
        super().__init__(source)
        self.raw_data_file_path = raw_data_file_path

    def import_data(self) -> pd.DataFrame:
        logger.info(f"Importing data from {self.source}...")
        html = self._get_html()
        if self.raw_data_file_path:
            self._store_raw_data(self.raw_data_file_path, html)
        df = self._parse_html(html)
        logger.info(f"Data imported successfully")
        return df

    def _get_html(self) -> str:
        """
        Fetch HTML from the given URL
        """
        try:
            response = requests.get(self.source)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"ERROR: Failed to fetch HTML from {self.source}")
            logger.error(f"ERROR: {e}")
            raise e

    @abstractmethod
    def _parse_html(self, html: str) -> pd.DataFrame:
        """
        Parse HTML to dataframe
        """
        pass

    def _store_raw_data(self, path: str, data: str):
        """
        Store raw data to the given file
        """
        with open(path, "w") as file:
            file.write(data)


class WikiWebImporter(WebImporterInterface):
    """
    Wiki Web Data Importer class.
    """

    IMF_WIKI_URL = (
        "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
    )
    COUNTRY_REGION_TABLE_PATH = (
        Path(__file__).resolve().parent / "../data/country_region_table.json"
    )

    def __init__(self, raw_data_file_path: str = None):
        super().__init__(self.IMF_WIKI_URL, raw_data_file_path)

    def import_data(self) -> pd.DataFrame:
        return super().import_data()

    # 여기서 region까지 매핑하는 것이 좋을까? .. yes
    # why? data importer가 가져오는 데이터의 포맷을 통일하고 싶다.
    def _parse_html(self, html: str) -> pd.DataFrame:
        data = self._parse_wiki_table_to_df(html)
        data = self._map_region(data)
        return data

    def _parse_wiki_table_to_df(self, html: str) -> pd.DataFrame:
        """
        Parse wikitable to dataframe
        """
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_="wikitable")
        data = []
        try:
            if not table:
                raise Exception("HTML Parsing Error. Wikitable not found")
            rows = table.find_all("tr")
            for row in rows[3:]:
                columns = row.find_all("td")
                country = columns[0].text.strip()
                gdp = columns[1].text.strip()
                if not gdp or gdp == "—":
                    continue
                data.append({"Country": country, "GDP": gdp})
            # TODO: dataframe 만들기 최적화
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"ERROR: Error parsing HTML: {e}")
            raise e

    def _map_region(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map region to the given country
        """

        country_region_df = pd.read_json(self.COUNTRY_REGION_TABLE_PATH, orient="index")
        country_region_df = country_region_df.reset_index()
        country_region_df.columns = ["Country", "Region"]

        df = df.merge(country_region_df, on="Country", how="left")

        return df


class FileImporter(ImporterInterface):
    """
    File data importer

    Read file and parse it to dataframe.
    """

    def import_data(self) -> pd.DataFrame:
        logger.info(f"Importing data from {self.source}...")
        df = self._parse_file(self.source)
        logger.info("Data imported successfully")
        return df

    @abstractmethod
    def _parse_file(self, file: str) -> pd.DataFrame:
        """
        Parse file to dataframe
        """
        pass


class CsvFileImporter(FileImporter):
    """
    Csv file importer
    """

    def _parse_file(self, file: str) -> pd.DataFrame:
        schema = {
            "Country": str,
            "GDP": str,
            "Region": str,
        }
        # df = pd.read_csv(file, dtype=schema, header=None, names=schema.keys())
        chunks = pd.read_csv(
            file, dtype=schema, header=None, names=schema.keys(), chunksize=100_000
        )
        df = pd.concat(chunks)
        return df
