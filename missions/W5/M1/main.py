import json
import os
from enum import IntEnum

from pyspark.sql import SparkSession

SPARK_HOME = os.getenv("SPARK_HOME", "")
SPARK_DATA = (
    os.path.join(SPARK_HOME, "data")
    if SPARK_HOME
    else os.path.join(os.getenv("HOME"), "docker/volumes/spark/data")
)


class TaxiColumns(IntEnum):
    VENDOR_ID = 0
    PICKUP_DATETIME = 1
    DROPOFF_DATETIME = 2
    PASSENGER_COUNT = 3
    TRIP_DISTANCE = 4
    RATECODE_ID = 5
    STORE_FWD_FLAG = 6
    PU_LOCATION_ID = 7
    DO_LOCATION_ID = 8
    PAYMENT_TYPE = 9
    FARE_AMOUNT = 10
    EXTRA = 11
    MTA_TAX = 12
    TIP_AMOUNT = 13
    TOLLS_AMOUNT = 14
    IMPROVEMENT_SURCHARGE = 15
    TOTAL_AMOUNT = 16
    CONGESTION_SURCHARGE = 17
    AIRPORT_FEE = 18


C = TaxiColumns


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
        os.path.join(SPARK_DATA, "source/yellow_2024_11.parquet")
    )

    rdd = df.rdd

    # fare amount가 0보다 크고 pickup_datetime이 2024년인 데이터만 남김
    rdd = rdd.filter(
        lambda x: x[C.FARE_AMOUNT] > 0 and x[C.PICKUP_DATETIME].year == 2024
    )

    rdd = rdd.cache()

    total_trip, total_revenue, total_trip_distance = rdd.map(
        lambda x: (1, x[C.FARE_AMOUNT], x[C.TRIP_DISTANCE])
    ).reduce(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))

    average_trip_distance = total_trip_distance / total_trip

    stats = {
        "total_trip": total_trip,
        "total_revenue": total_revenue,
        "average_trip_distance": average_trip_distance,
    }

    os.makedirs(os.path.join(SPARK_DATA, "output/rdd/metrics"), exist_ok=True)

    # print(total_trip, total_revenue, avg_trip_dist)
    with open(
        os.path.join(SPARK_DATA, "output/rdd/metrics/stats.json"), "w"
    ) as f:
        json.dump(stats, f)

    rdd = rdd.map(
        lambda x: (x[C.PICKUP_DATETIME].date(), (1, x[C.FARE_AMOUNT]))
    ).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    df = rdd.map(lambda x: (x[0], x[1][0], x[1][1])).toDF(
        ["date", "trip_count", "revenue"]
    )

    df.write.save(
        os.path.join(SPARK_DATA, "output/rdd/metrics/daily"),
        "parquet",
        "overwrite",
    )


if __name__ == "__main__":
    main()
