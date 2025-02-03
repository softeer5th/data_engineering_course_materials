import argparse
import os

from urllib.error import HTTPError
from urllib.request import urlopen

BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
BLOCK_SIZE = 1024 * 1024

parser = argparse.ArgumentParser(prog='download_NYC_TLC')
parser.add_argument('vehicle_type', type=str, choices=['yellow', 'green', 'fhvhv'])
parser.add_argument('year_start', type=int, default=2009)
parser.add_argument('year_end', type=int, default=2024)


if __name__ == '__main__':
    args = parser.parse_args()

    vehicle_type = args.vehicle_type
    year_start = args.year_start
    year_end = args.year_end

    if not os.path.exists('./data/NYC-TLC'):
        os.mkdir('./data/NYC-TLC')

    for year in range(year_start, year_end + 1):
        for month in range(1, 13):
            filename = f'{vehicle_type}_tripdata_{year}-{month:02d}.parquet'

            download_msg = f'Downloading {filename} from NYC.gov... '
            print(download_msg, end='')

            try:
                with urlopen(BASE_URL + filename) as response:
                    with open(f'./data/NYC-TLC/{filename}', 'wb') as f:
                        filesize_mb = 1
                        bytes_block = response.read(BLOCK_SIZE)
                        while len(bytes_block) > 0:
                            f.write(bytes_block)
                            filesize_mb += 1
                            bytes_block = response.read(BLOCK_SIZE)
                            print('\r' + download_msg, filesize_mb, "MB", end='')
            except HTTPError as e:
                print('\r' + download_msg + str(e))
            else:
                print('\r' + download_msg + 'Done!            ')

# 사용 예시
# python download_tlc_data.py yellow 2022 2024