import pandas as pd
import datetime
import sqlite3

# World_Economies.db로 데이터 로드
def connect_and_load(df, log_file):
    try:
        with sqlite3.connect("missions/W1/M3/World_Economies.db") as conn: #db와 연결 - 없으면 새로 생성됨
            print(f"Opened SQLite database with version {sqlite3.sqlite_version} successfully.")
            df.to_sql('Countries_by_GDP', conn, if_exists='replace') #이미 있던 테이블 제거 후 저장
            print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Load) 변환된 데이터 Load", file=log_file)       

    except sqlite3.OperationalError as e:
        print("Failed to open database:", e)