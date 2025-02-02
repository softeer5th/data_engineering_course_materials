#!/bin/bash

SPARK_MASTER="spark://spark-master:7077"

SPARK_JOB_PATH='/opt/spark-data/pi.py'

spark-submit \
    --master $SPARK_MASTER \
    $SPARK_JOB_PATH 100000