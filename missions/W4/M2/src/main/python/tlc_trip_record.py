import numpy as np
from pyspark.sql import SparkSession
import pyspark.pandas as ps

# def data_ingestion(spark, input_path):
#     df = spark.read.csv(input_path, header=True, inferSchema=True)
#     return df

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-01.parquet"
spark = SparkSession.builder.appName("tlc_trip_record").getOrCreate()
df = ps.read_parquet(url)
