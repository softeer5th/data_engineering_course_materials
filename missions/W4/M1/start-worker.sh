#!/bin/bash
SPARK_MASTER_URL=spark://master:7077
$SPARK_HOME/sbin/start-slave.sh $SPARK_MASTER_URL
tail -f $SPARK_HOME/logs/spark--org.apache.spark.deploy.worker.Worker-*.out
