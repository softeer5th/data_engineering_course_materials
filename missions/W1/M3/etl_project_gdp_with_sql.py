# 표준 라이브러리
import sqlite3

# 서드파티 라이브러리
import pandas as pd

# 로컬 모듈
import missions.W1.M3.etl_project_gdp as etl_basic
from missions.W1.M3.log.log import Logger

logger = Logger.get_logger("GDP_ETL_LOGGER")

###########
#  Extract Process
###########
def extract_gdp_wiki_web2json(url: str, out_file: str) -> None:
    etl_basic.extract_gdp_wiki_web2json(url, out_file)

###########
#  Transform Process
###########

def _json2dataframe(json_file: str)-> pd.DataFrame:
    """
    Reads a JSON file and converts it into a Pandas DataFrame.

    Args:
        json_file (str): The file path to the JSON file to be read.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the data from the JSON file.
    """

    try:
        logger.info(f"[START] Read data from {json_file} ...")
        df = pd.read_json(json_file)
        logger.info(f"[COMPLETE] Data was read successfully: {json_file}")
    except Exception as e:
        logger.error(f"[FAIL] Failed to read data to {json_file}. Exception: {e.__class__.__name__}")
        raise
    return df

def _merge_region_by_country(df_gdp: pd.DataFrame, df_region: pd.DataFrame)-> pd.DataFrame:
    """
    Merges two DataFrames: GDP data and region data, based on the 'country' column.

    Args:
        df_gdp (pd.DataFrame): A Pandas DataFrame containing GDP data with a 'country' column.
        df_region (pd.DataFrame): A Pandas DataFrame containing region data with a 'country' column.

    Returns:
        pd.DataFrame: A merged DataFrame that includes GDP data combined with corresponding region data.
    """
    try:
        logger.info(f"[START] Start Merging the region data with gdp data ...")
        df_merged = df_gdp.merge(df_region, on='country', how='left')
        logger.info(f"[COMPLETE] Succesfuly merged the region data with gdp data.")
    except Exception as e:
        logger.info(f"[FAIL] Failed to merge the region data with gdp data.")
        raise
    return df_merged

def _transform_gdp(df_gdp):
    """
    Transforms the GDP data in a DataFrame.

    Args:
        df_gdp (pd.DataFrame): A Pandas DataFrame containing a 'gdp' column 
                               with GDP data as strings.

    Returns:
        pd.DataFrame: A transformed DataFrame with numeric and sorted GDP data.
    """
    # DataFrame으로 Transform하는 것은 분산 환경에 대응 가능한가? to_sql과 마찬가지로 메모리 및 CPU 성능 이슈가 발생할 것 같다.

    df_gdp['gdp'] = pd.to_numeric(df_gdp['gdp'].str.replace(",", ""), errors='coerce')  # str2int
    df_gdp['gdp'] = (df_gdp['gdp'] / 1e3).round(2)  # GDP의 단위는 1B USD이어야 하고 소수점 2자리까지만 표시해 주세요.
    df_gdp = df_gdp.sort_values(by='gdp', ascending=False).reset_index(drop=True)  # 해당 테이블에는 GDP가 높은 국가들이 먼저 나와야 합니다.

    # 결측값에 "측정하지 못한 통게치"라는 의미가 있기 때문에 처리하지 않는다.
        

def transform_gdp_from_json(gdp_file, region_file):
    """
    Transforms GDP data from JSON files and merges it with region data.
    
    Args:
        gdp_file (str): The file path to the JSON file containing GDP data.
        region_file (str): The file path to the JSON file containing region data.

    Returns:
        pd.DataFrame: A DataFrame containing the transformed GDP data combined with region data.
    """

    try:
        logger.info("[START] Starting GDP Transforming process...")

        df_gdp = _json2dataframe(gdp_file)
        df_region = _json2dataframe(region_file)

        # 데이터 정제 및 변환
        _transform_gdp(df_gdp)

        # Region 병합
        df_gdp_with_region = _merge_region_by_country(df_gdp, df_region)

        logger.info("[COMPLETE] GDP Transforming process completed successfully.")
    except Exception as e:
          logger.warn("[FAIL] GDP Transforming process failed.")
    
    return df_gdp_with_region

# ----------------------
# 3. Load: 데이터 로드
# ----------------------
def _create_table(db_name:str)-> None:
    """
    Creates a SQLite table named 'Countries_by_GDP' if it does not already exist.

    Args:
        db_name (str): The name of the SQLite database file.

    Returns:
        None
    """

    logger.info("[START] Creating a SQLite DB Table ...")
    with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()

            create_query = '''
                CREATE TABLE IF NOT EXISTS Countries_by_GDP (
                Country TEXT,
                GDP_USD_billion REAL,
                Year TEXT,
                Type TEXT,
                Region TEXT
            )
            '''
            cursor.execute(create_query)
    logger.info("[COMPLETE] Successfuly created a SQLite DB Table.")

def _mapping_colums(df: pd.DataFrame, db_name:str):
    with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()

            cursor.execute("PRAGMA table_info(Countries_by_GDP);")
            table_info = cursor.fetchall()

            table_columns = [info[1] for info in table_info]
            column_mapping = {df_col: tbl_col for df_col, tbl_col in zip(df.columns, table_columns)}
            return df.rename(columns=column_mapping)

def _insert_all_into_table(df: pd.DataFrame, db_name: str)-> None:
    """
    Inserts all data from a DataFrame into the 'Countries_by_GDP' SQLite table.

    Args:
        df (pd.DataFrame): A Pandas DataFrame containing the data to be inserted.
        db_name (str): The file path to the SQLite database.

    Returns:
        None
    """

    logger.info("[START] Inserting all data into the SQLite DB Table ...")
    with sqlite3.connect(db_name) as conn:
            
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(Countries_by_GDP);")
            table_info = cursor.fetchall()
            table_columns = [info[1] for info in table_info]
            column_mapping = {df_col: tbl_col for df_col, tbl_col in zip(df.columns, table_columns)}
            df = df.rename(columns=column_mapping)

            df.to_sql("Countries_by_GDP", conn, if_exists="append", index=False)
    logger.info("[COMPLETE] Successfuly inserted all data into the SQLite DB Table.")

def load_to_sqlite(df_gdp: pd.DataFrame, db_name:str ="missions/W1/M3/data/Countries_by_GDP.db")-> None:
    """
    Loads GDP data into a SQLite database.

    Args:
        df_gdp (pd.DataFrame): A Pandas DataFrame containing GDP data to load.
        db_name (str): The file path to the SQLite database.
    """
    try:
        logger.info("[START] Starting GDP Load process into SQLite DB...")
        _create_table(db_name)
        mapping_df_gdp = _mapping_colums(df_gdp, db_name)
        _insert_all_into_table(mapping_df_gdp, db_name)
        logger.info("[COMPLETE] GDP Load process completed successfully.")
    except Exception as e:
        logger.warn("[FAIL] GDP Load process failed.")
        raise

# ----------------------
# Main ETL Process
# ----------------------

def main():
    """
    Executes the GDP ETL (Extract, Transform, Load) process.

    Args:
        None

    Returns:
        None
    """
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
    gdp_file = "missions/W1/M3/data/Countries_by_GDP.json"
    region_file = "missions/W1/M3/data/cultural-geo-mapper.json"

    try:
        logger.info("[START] Starting GDP ETL Process ...")
       
        extract_gdp_wiki_web2json(url, gdp_file)
        load_to_sqlite(transform_gdp_from_json(gdp_file, region_file))

        logger.info("[COMPLETE] GDP ETL Process completed successfully.")
    except Exception as e:
        logger.warn("[FAIL] GDP ETL Process failed.")
        raise

if __name__ == "__main__":
    main()
