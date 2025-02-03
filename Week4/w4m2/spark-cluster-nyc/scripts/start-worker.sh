#!/bin/bash
SPARK_MASTER_URL=$1
/opt/spark/sbin/start-worker.sh $SPARK_MASTER_URL
tail -f /opt/spark/logs/spark--org.apache.spark.deploy.worker.Worker-*.out
