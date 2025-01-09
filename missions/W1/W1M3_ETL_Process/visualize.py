import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from utils import log_message
import os

DB_FILE = 'sqliteDB/World_Economies.db'
PLOT_PATH = 'results/plots/'

def visualize_gdp_over_100b():
    query = """
    SELECT Country, GDP_USD_billion, Year
    FROM Countries_by_GDP
    WHERE GDP_USD_billion >= 100
    ORDER BY GDP_USD_billion DESC;
    """
    
    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql(query, conn)
    
    plt.figure(figsize=(12, 6))
    plt.barh(df['Country'], df['GDP_USD_billion'], color='skyblue')
    plt.xlabel('GDP (Trillion USD)')
    plt.ylabel('Country')
    plt.title('GDP Over 100 Billion USD')
    plt.gca().invert_yaxis()  # 상위 국가가 위로 오도록
    plt.tight_layout()

    os.makedirs(PLOT_PATH, exist_ok=True)
    file_path = os.path.join(PLOT_PATH, 'gdp_over_100b.png')
    plt.savefig(file_path)
    plt.close()

    log_message(f"GDP 상위 100B USD 국가 시각화 완료: {file_path}")


def visualize_region_avg_gdp():
    query = """
    SELECT Region, ROUND(AVG(GDP_USD_billion), 2) AS Avg_GDP
    FROM (
        SELECT Country, Region, GDP_USD_billion,
               ROW_NUMBER() OVER (PARTITION BY Region ORDER BY GDP_USD_billion DESC) AS rank
        FROM Countries_by_GDP
    )
    WHERE rank <= 5
    GROUP BY Region
    ORDER BY Avg_GDP DESC;
    """
    
    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql(query, conn)

    plt.figure(figsize=(10, 5))
    plt.bar(df['Region'], df['Avg_GDP'], color='orange')
    plt.xlabel('Region')
    plt.ylabel('Average GDP (Trillion USD)')
    plt.title('Region-wise Top 5 Avg GDP')
    plt.xticks(rotation=45)
    plt.tight_layout()

    file_path = os.path.join(PLOT_PATH, 'region_avg_gdp.png')
    plt.savefig(file_path)
    plt.close()

    log_message(f"Region별 평균 GDP 시각화 완료: {file_path}")


if __name__ == '__main__':
    visualize_gdp_over_100b()
    visualize_region_avg_gdp()