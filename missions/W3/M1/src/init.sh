#!/bin/bash

# NameNode 포맷 (최초 실행 시에만)
if [ ! -d "/opt/hadoop_data/hdfs/namenode" ] || [ -z "$(ls -A /opt/hadoop_data/hdfs/namenode)" ]; then
    echo "Formatting NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force
else
    echo "NameNode already formatted. Skipping format."
fi

# HDFS 및 YARN 서비스 시작
$HADOOP_HOME/bin/hdfs namenode &
$HADOOP_HOME/bin/hdfs datanode &

sleep 10

$HADOOP_HOME/bin/hdfs dfs -mkdir -p /data/uploads
$HADOOP_HOME/bin/hdfs dfs -chown hdfs:hdfs /data/uploads
$HADOOP_HOME/bin/hdfs dfs -chmod 777 /data/uploads

# 포그라운드 유지
tail -f /dev/null