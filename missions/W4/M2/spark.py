from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder.appName("NYC Taxi Data Analysis").getOrCreate()

def load_taxi_data(spark: SparkSession, data_path: str):
    return spark.read.parquet(f"{data_path}/*.parquet")

def main():
    spark = create_spark_session()

    df = load_taxi_data(spark, "data")

    df.printSchema()

    print(f"Total number of records: {df.count()}")

    spark.stop()

if __name__ == "__main__":
    main()