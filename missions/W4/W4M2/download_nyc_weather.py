import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta

API_URL = "https://archive-api.open-meteo.com/v1/archive"
NYC_LAT, NYC_LON = 40.7128, -74.0060
OUTPUT_FILE = "data/nyc_weather.csv"

def fetch_weather_data(start_year, end_year):
    weather_data = []
    
    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
        params = {
            "latitude": NYC_LAT,
            "longitude": NYC_LON,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
            "timezone": "America/New_York"
        }
        
        response = requests.get(API_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            for i, date in enumerate(data["daily"]["time"]):
                weather_data.append({
                    "date": date,
                    "temp_max": data["daily"]["temperature_2m_max"][i],
                    "temp_min": data["daily"]["temperature_2m_min"][i],
                    "precipitation": data["daily"]["precipitation_sum"][i]
                })
        else:
            print(f"Failed to fetch data for {year}: {response.status_code}")
    
    return weather_data

def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Weather data saved to {filename}")

def main():
    parser = argparse.ArgumentParser(description="Download NYC weather data")
    parser.add_argument("--start", type=int, required=True, help="Start year (e.g., 2020)")
    parser.add_argument("--end", type=int, required=True, help="End year (e.g., 2022)")
    args = parser.parse_args()
    
    weather_data = fetch_weather_data(args.start, args.end)
    save_to_csv(weather_data, OUTPUT_FILE)

if __name__ == "__main__":
    main()