import requests
import zipfile
from tqdm import tqdm
import os

# URL 및 파일 경로 설정
url = "https://files.grouplens.org/datasets/movielens/ml-20m.zip"
zip_name = "ml-20m.zip"
csv_name = "ml-20m/ratings.csv"

def download_file(url, zip_name):
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    chunk_size = 1024
    with open(zip_name, "wb") as file, tqdm(
        desc=f"Downloading {zip_name}",
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                file.write(chunk)
                bar.update(len(chunk))  # 진행률 바 업데이트

def extract_zip(file_name):
    with zipfile.ZipFile(file_name, "r") as zipf:
        if csv_name in zipf.namelist():
            zipf.extract(csv_name)
        else:
            print('CSV file is not found in the ZIP file.')

def main():
    if not os.path.exists(zip_name):
        download_file(url, zip_name)
    else:
        print("zip file already exists.")

    if not os.path.exists(csv_name):
        extract_zip(zip_name)
    else:
        print("file already exists.")

if __name__ == "__main__":
    main()
