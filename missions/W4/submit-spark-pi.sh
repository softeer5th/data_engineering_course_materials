#!/bin/bash

SPARK_MASTER="spark://spark-master:7077"

SPARK_JOB_PATH='/tmp/pi.py'

/opt/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    $SPARK_JOB_PATH 100000