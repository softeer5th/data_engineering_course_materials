from abc import ABC, abstractmethod

from app.core.constants import DATAFRAME_SCHEMA
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)


class Transformer(ABC):
    @abstractmethod
    def __init__(self, df: DataFrame):
        self.df = df

    @abstractmethod
    def transform(self) -> DataFrame:
        return self.df


class DataStandardizer(Transformer):
    def __init__(self, dfs: list[DataFrame]):
        self.dfs = dfs

    def _standardize_single_df(self, df: DataFrame) -> DataFrame:
        """단일 DataFrame을 표준화하는 메서드"""
        # 1. taxi_type 추가
        df = df.withColumn(
            "taxi_type",
            F.when(F.input_file_name().contains("yellow"), "yellow").otherwise(
                "green"
            ),
        )

        # 2. 컬럼명 소문자 변환
        for old_col in df.columns:
            new_col = old_col.lower()
            df = df.withColumnRenamed(old_col, new_col)

        # 3. 현재 컬럼 확인
        cols = set(df.columns)

        # 4. datetime 컬럼 변환
        datetime_mapping = {
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
        }

        for old_col, new_col in datetime_mapping.items():
            if old_col in cols:
                df = df.withColumnRenamed(old_col, new_col)

        # 5. 필수 컬럼 선택 (모든 스키마에 공통적으로 존재)
        select_exprs = [
            F.col("taxi_type").cast(StringType()),
            F.col("vendorid").cast(IntegerType()).alias("vendor_id"),
            F.col("pickup_datetime").cast(TimestampType()),
            F.col("dropoff_datetime").cast(TimestampType()),
            F.col("store_and_fwd_flag").cast(StringType()),
            F.col("ratecodeid").cast(IntegerType()).alias("ratecode_id"),
            F.col("pulocationid").cast(IntegerType()).alias("pu_location_id"),
            F.col("dolocationid").cast(IntegerType()).alias("do_location_id"),
            F.col("passenger_count").cast(IntegerType()),
            F.col("trip_distance").cast(DoubleType()),
            F.col("fare_amount").cast(DoubleType()),
            F.col("extra").cast(DoubleType()),
            F.col("mta_tax").cast(DoubleType()),
            F.col("tip_amount").cast(DoubleType()),
            F.col("tolls_amount").cast(DoubleType()),
            F.col("improvement_surcharge").cast(DoubleType()),
            F.col("total_amount").cast(DoubleType()),
            F.col("payment_type").cast(IntegerType()),
            F.col("congestion_surcharge").cast(DoubleType()),
        ]

        # 6. Optional 컬럼 추가
        if "airport_fee" in cols:  # yellow taxi only
            select_exprs.append(F.col("airport_fee").cast(DoubleType()))
        else:
            select_exprs.append(
                F.lit(0.0).cast(DoubleType()).alias("airport_fee")
            )

        if "ehail_fee" in cols:  # green taxi only
            select_exprs.append(F.col("ehail_fee").cast(DoubleType()))
        else:
            select_exprs.append(
                F.lit(0.0).cast(DoubleType()).alias("ehail_fee")
            )

        if "trip_type" in cols:  # green taxi only
            select_exprs.append(F.col("trip_type").cast(IntegerType()))
        else:
            select_exprs.append(
                F.lit(0).cast(IntegerType()).alias("trip_type")
            )

        # 7. 최종 변환
        return df.select(select_exprs)

    def transform(self) -> DataFrame:
        """모든 DataFrame을 표준화하고 병합하는 메서드"""
        # 각 DataFrame을 표준화
        standardized_dfs = [self._standardize_single_df(df) for df in self.dfs]

        # 표준화된 DataFrame들을 병합
        if len(standardized_dfs) == 1:
            return standardized_dfs[0]

        # unionAll을 사용하여 모든 DataFrame 병합
        result_df = standardized_dfs[0]
        for df in standardized_dfs[1:]:
            result_df = result_df.unionAll(df)

        return result_df


class DataCleaner(Transformer):
    def __init__(self, df: DataFrame):
        self.df = df

    def _handle_missing_values(self):
        self.df = self.df.na.drop()
        return self.df

    def _handle_outliners(self):
        self.df = self.df.filter("fare_amount > 0")
        return self.df

    def _standardize_timestamps(self):
        self.df = self.df.withColumn(
            "pickup_datetime", self.df["pickup_datetime"].cast("timestamp")
        )
        return self.df

    def transform(self) -> DataFrame:
        self._handle_missing_values()

        return self.df


class MetricCalculator(Transformer):
    def __init__(self, df: DataFrame):
        self.df = df

    def transform(self): ...
