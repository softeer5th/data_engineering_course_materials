from functools import partial
import pandas as pd
from multiprocessing import Pool

def read_parquet_chunk(file_path: str, offset: int, size: int) -> pd.DataFrame:
    return pd.read_parquet(
        file_path,
        offset=offset,
        rows=size
    )

def read_country_chunk(data_dir: str, year_range: range, offset: int, size: int) -> pd.DataFrame:
    chunk_parts = []

    for year in year_range:
        file_path = f"{data_dir}/gdp_{year}.parquet"
        chunk_part = read_parquet_chunk(file_path, offset, size)
        # 청크를 합쳐서 행 인덱스가 국가, 열 인덱스가 연도인 데이터프레임 생성
        chunk_parts.append(chunk_part)

    return pd.concat(chunk_parts, axis=1)

def analyze_gdp_data(data_dir: str, year: int) -> None:
    """
    Parquet 파일을 읽어서 기본 통계량을 출력합니다.
    
    Args:
        data_dir (str): 파일이 저장된 디렉토리 경로
        year (int): 분석할 연도
    """
    # Parquet 파일 읽기
    df = pd.read_parquet(f"{data_dir}/gdp_{year}.parquet")
    
    print(f"=== {year}년 GDP 데이터 분석 ===")
    print("\n1. 데이터 크기:")
    print(f"행: {df.shape[0]}, 열: {df.shape[1]}")
    
    print("\n2. 컬럼 목록:")
    print(df.columns.tolist())
    
    print("\n3. 기본 통계량:")
    print(df.describe())
    
    print("\n4. 결측치 현황:")
    print(df.isnull().sum())