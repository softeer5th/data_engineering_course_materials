#!/bin/bash
$SPARK_HOME/sbin/start-master.sh
tail -f $SPARK_HOME/logs/spark--org.apache.spark.deploy.master.Master-*.out
