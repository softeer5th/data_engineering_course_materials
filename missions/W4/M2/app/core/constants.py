import os
from enum import Enum

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SPARK_HOME = os.getenv("SPARK_HOME", "")
SPARK_DATA = (
    os.path.join(SPARK_HOME, "data")
    if SPARK_HOME
    else os.path.join(os.getenv("HOME"), "docker/volumes/spark/data")
)

TRIP_DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{year}-{month:02d}.parquet"

LOG_DIR = os.path.join(SPARK_DATA, "log/")

CLEANED_DATA_DIR = os.path.join(SPARK_DATA, "cleaned/")
ANALYZED_DATA_DIR = os.path.join(SPARK_DATA, "analyzed/")
SOURCE_DATA_DIR = os.path.join(SPARK_DATA, "source/")
SOURCE_FILE_PATH = os.path.join(
    SOURCE_DATA_DIR, "{type}_{year}_{month:02d}.parquet"
)


class VehicleType(str, Enum):
    GREEN = "green"
    YELLOW = "yellow"


def get_trip_data_url(vehicle_type: VehicleType, year: int, month: int) -> str:
    return TRIP_DATA_URL.format(
        type=vehicle_type.value, year=year, month=month
    )


def get_trip_data_path(
    vehicle_type: VehicleType, year: int, month: int
) -> str:
    return SOURCE_FILE_PATH.format(
        type=vehicle_type.value, year=year, month=month
    )


DATAFRAME_SCHEMA = StructType(
    [
        StructField("taxi_type", StringType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("ratecode_id", IntegerType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("trip_type", IntegerType(), True),
        StructField("ehail_fee", DoubleType(), True),
    ]
)
