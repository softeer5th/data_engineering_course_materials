from app.core.loader import Loader
from app.core.transformer import DataStandardizer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


def create_spark_session() -> SparkSession:
    builder: SparkSession.Builder = SparkSession.builder
    return (
        builder.appName("NYC Taxi Data Processing")
        # 메모리 설정
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.cores", "1")
        # UI 비활성화
        .config("spark.ui.enabled", "false")
        # Parquet 관련 설정
        .config(
            "spark.sql.parquet.recordLevelCounter.enabled", "false"
        )  # 레코드 레벨 카운터 비활성화
        .config("spark.sql.parquet.blockSize", "16m")  # 블록 크기 감소
        .config("spark.sql.parquet.pageSize", "1m")  # 페이지 크기 감소
        # Write 관련 설정
        .config(
            "spark.sql.files.maxRecordsPerFile", "100000"
        )  # 파일당 최대 레코드 수 제한
        .config("spark.sql.shuffle.partitions", "10")  # 셔플 파티션 수 제한
        # 압축 설정
        .config(
            "spark.sql.parquet.compression.codec", "snappy"
        )  # 가벼운 압축 방식 사용
        .getOrCreate()
    )


import glob
import os

import pyarrow.parquet as pq
from app.core.constants import SOURCE_DATA_DIR


def check_unique_schemas():
    parquet_files = glob.glob(os.path.join(SOURCE_DATA_DIR, "*.parquet"))
    schema_to_files = {}

    for file_path in parquet_files:
        # 파일의 메타데이터만 읽어와서 스키마 확인
        schema = pq.read_schema(file_path)
        schema_str = str(schema)

        if schema_str not in schema_to_files:
            schema_to_files[schema_str] = []
        schema_to_files[schema_str].append(os.path.basename(file_path))

    for i, (schema_str, files) in enumerate(schema_to_files.items(), 1):
        print(f"\nSchema variant {i}:")
        print(
            "Files:",
            ", ".join(files[:3]),
            f"... (total {len(files)} files)" if len(files) > 3 else "",
        )
        print("Schema:", schema_str)


def main():
    spark = create_spark_session()

    # spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
    # spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")

    loader = Loader(spark)

    dfs = loader.load_source_list()

    standardizer = DataStandardizer(dfs)

    standardized_df = standardizer.transform()

    standardized_df.printSchema()

    loader.save_cleaned(standardized_df)

    spark.stop()


if __name__ == "__main__":
    main()
    # check_unique_schemas()
