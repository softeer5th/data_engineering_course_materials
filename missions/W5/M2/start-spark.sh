#!/bin/bash

# SSH 서비스 시작
service ssh start

# HDFS 포맷 (최초 실행 시에만 수행)
if [ ! -d "/hadoop/dfs/name/current" ]; then
  $HADOOP_HOME/bin/hdfs namenode -format
fi

# Hadoop 서비스 시작
$HADOOP_HOME/sbin/start-dfs.sh

# Spark 서비스 시작
if [ "$SPARK_MODE" = "master" ]; then
  $SPARK_HOME/sbin/start-master.sh
elif [ "$SPARK_MODE" = "worker" ]; then
  $SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER_URL
fi

# 컨테이너를 계속 실행 상태로 유지
tail -f /dev/null