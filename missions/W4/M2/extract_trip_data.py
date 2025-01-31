from concurrent.futures import ThreadPoolExecutor
import requests
import os

# 데이터 저장 경로 설정
data_dir = "./NYC_TLC_Trip_Data"
os.makedirs(data_dir, exist_ok=True)

# 연도 및 월 설정
years = range(2022, 2025)  # 필요한 연도 범위
months = range(1, 13)  # 1월부터 12월까지

# URL 템플릿
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

# 파일 다운로드 함수
def download_file(year, month):
    file_url = base_url.format(year=year, month=month)
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    file_path = os.path.join(data_dir, file_name)
    
    try:
        print(f"Downloading {file_url}...")
        response = requests.get(file_url, stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"Downloaded: {file_name}")
        else:
            print(f"File not found: {file_url}")
    except Exception as e:
        print(f"Error downloading {file_url}: {e}")

# 모든 연도 및 월 데이터 다운로드
for year in years:
    for month in months:
        download_file(year, month)
