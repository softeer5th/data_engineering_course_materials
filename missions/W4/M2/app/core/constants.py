from enum import Enum

TRIP_DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{year}-{month:02d}.parquet"
TRIP_DATA_SAVE_PATH = "data/{type}_{year}_{month:02d}.parquet"

class VehicleType(str, Enum):
    GREEN = "green"
    YELLOW = "yellow"

def get_trip_data_url(vehicle_type: VehicleType, year: int, month: int) -> str:
    return TRIP_DATA_URL.format(type=vehicle_type.value, year=year, month=month)

def get_data_save_path(vehicle_type: VehicleType, year: int, month: int) -> str:
    return TRIP_DATA_SAVE_PATH.format(type=vehicle_type.value, year=year, month=month)