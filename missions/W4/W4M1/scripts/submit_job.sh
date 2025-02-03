#!/bin/bash

# Spark 마스터 주소
SPARK_MASTER="spark://spark-master:7077"

# 실행할 Spark 스크립트
SPARK_SCRIPT="/opt/spark/scripts/pi.py"

# 결과 저장 경로
OUTPUT_PATH="/data/output"

# Spark 작업 제출
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    --deploy-mode client \
    $SPARK_SCRIPT $OUTPUT_PATH