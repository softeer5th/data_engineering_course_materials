from pyspark.sql import SparkSession
from pyspark.sql.functions import count, to_date, sum, avg
import os

spark = (
    SparkSession.builder.appName("M01")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", f"{os.getcwd()}/spark-events")
    .config("spark.history.fs.logDirectory", f"{os.getcwd()}/spark-events")
    .getOrCreate()
)

df = spark.read.parquet("data/yellow_tripdata_2024-01.parquet")

# df.printSchema()

df = df.dropna(
    subset=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
    ]
)
df = df.filter(df.fare_amount > 0)
df = df.filter(df.trip_distance > 0)

# datetime to date
df = df.withColumn("date", to_date(df.tpep_pickup_datetime))
df = df.filter(df.date < "2024-02-01")
df = df.filter(df.date >= "2024-01-01")


# Calculate and display the total number of trips.
print("Total number of trips: ", df.count())

# Calculate and display the total revenue generated from the trips.
total_revenue = df.select(sum(df.total_amount)).collect()[0][0]
print("Total revenue: ", total_revenue)

# # Calculate and display the average trip distance.
average_trip_distance = df.select(avg(df.trip_distance)).collect()[0][0]
print("Average trip distance: ", average_trip_distance)

# # Calculate and display the number of trips per day.
trips_per_day_df = df.groupBy("date").agg(count("*").alias("total_trips"))
trips_per_day_df.show()

# # Calculate and display the total revenue per day.
total_revenue_per_day = df.groupBy("date").agg(
    sum("total_amount").alias("total_revenue")
)
total_revenue_per_day.show()