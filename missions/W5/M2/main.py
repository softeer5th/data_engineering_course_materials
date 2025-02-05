import json
import os
from enum import Enum
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SPARK_HOME = os.getenv("SPARK_HOME", "")
SPARK_DATA = (
    os.path.join(SPARK_HOME, "data")
    if SPARK_HOME
    else os.path.join(os.getenv("HOME"), "docker/volumes/spark/data")
)


class TaxiColumns(str, Enum):
    VENDOR_ID = "VendorID"
    PICKUP_DATETIME = "tpep_pickup_datetime"
    DROPOFF_DATETIME = "tpep_dropoff_datetime"
    PASSENGER_COUNT = "passenger_count"
    TRIP_DISTANCE = "trip_distance"
    RATECODE_ID = "RatecodeID"
    STORE_FWD_FLAG = "store_and_fwd_flag"
    PU_LOCATION_ID = "PULocationID"
    DO_LOCATION_ID = "DOLocationID"
    PAYMENT_TYPE = "payment_type"
    FARE_AMOUNT = "fare_amount"
    EXTRA = "extra"
    MTA_TAX = "mta_tax"
    TIP_AMOUNT = "tip_amount"
    TOLLS_AMOUNT = "tolls_amount"
    IMPROVEMENT_SURCHARGE = "improvement_surcharge"
    TOTAL_AMOUNT = "total_amount"
    CONGESTION_SURCHARGE = "congestion_surcharge"
    AIRPORT_FEE = "Airport_fee"


class WeatherColumns(str, Enum):
    DATETIME = "date"
    TEMPERATURE = "temperature_2m"
    RELATIVE_HUMIDITY = "relative_humidity_2m"
    DEW_POINT = "dew_point_2m"
    APPARENT_TEMPERATURE = "apparent_temperature"
    PRECIPITATION = "precipitation"
    WEATHER_CODE = "weather_code"


C = TaxiColumns

WC = WeatherColumns


def create_spark_session() -> SparkSession:
    builder: SparkSession.Builder = SparkSession.builder
    return (
        builder.appName("NYC Taxi Data Processing")
        # .config(
        #     "spark.driver.extraJavaOptions", "-Djava.security.manager=allow"
        # )
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    df = spark.read.parquet(
        # 1개월 분량으로 테스트
        os.path.join(SPARK_DATA, "source/yellow_2023_12.parquet"),
        os.path.join(SPARK_DATA, "source/yellow_2024_*.parquet"),
    )

    weather_df = spark.read.parquet(
        os.path.join(SPARK_DATA, "weather.parquet")
    )

    required_columns = [
        C.PICKUP_DATETIME,
        C.DROPOFF_DATETIME,
        C.PASSENGER_COUNT,
        C.TRIP_DISTANCE,
        C.FARE_AMOUNT,
    ]

    null_conditions = reduce(
        lambda x, y: x & y, [F.col(c).isNotNull() for c in required_columns]
    )

    valid_conditions = (
        (F.col(C.FARE_AMOUNT) > 0)
        & (F.col(C.TRIP_DISTANCE) > 0)
        & (F.col(C.TRIP_DISTANCE) < 100)
        & (F.col(C.PASSENGER_COUNT) > 0)
        & (F.year(C.PICKUP_DATETIME) == 2024)
    )

    df = df.filter(null_conditions & valid_conditions)

    df = df.cache()

    df = df.join(
        weather_df,
        F.date_trunc("hour", df[C.PICKUP_DATETIME])
        == F.date_trunc("hour", weather_df[WC.DATETIME]),
        "left",
    ).drop(F.col(WC.DATETIME))

    stats = (
        df.groupBy(F.col(WC.WEATHER_CODE))
        .agg(
            F.count("*").alias("total_trip"),
            F.mean(F.col(C.FARE_AMOUNT)).alias("avg_revenue"),
            F.mean(F.col(C.TRIP_DISTANCE)).alias("avg_trip_distance"),
        )
        .sort(F.col(WC.WEATHER_CODE))
    )

    stats.write.save(
        os.path.join(SPARK_DATA, "output/df/weather_stats.parquet"),
        "parquet",
        "overwrite",
    )


if __name__ == "__main__":
    main()
