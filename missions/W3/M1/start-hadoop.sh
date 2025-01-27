#!/bin/bash

# 루트 사용자 실행 허용을 위한 환경 변수 설정
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

# SSH 서비스 시작
service ssh start

# HDFS 포맷 (최초 실행 시만 필요)
if [ ! -d "/hadoop/dfs/name" ]; then
  echo "Formatting HDFS..."
  $HADOOP_HOME/bin/hdfs namenode -format
fi

# Hadoop 서비스 시작
echo "Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh
echo "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

# 컨테이너가 계속 실행되도록 유지
echo "Hadoop services started. Container is running..."
tail -f /dev/null
