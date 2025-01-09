import pandas as pd
import sqlite3
from utils import log_message

PROCESSED_DATA_FILE = 'results/Countries_by_GDP_Processed.json'
DB_FILE = 'sqliteDB/World_Economies.db'

def load():
    df = pd.read_json(PROCESSED_DATA_FILE)
    with sqlite3.connect(DB_FILE) as conn:
        df.to_sql('Countries_by_GDP', conn, if_exists='replace', index=False)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM Countries_by_GDP")
        row_count = cur.fetchone()[0]
    
    log_message(f"{row_count}개 행 데이터베이스에 적재 완료")

if __name__ == '__main__':
    load()