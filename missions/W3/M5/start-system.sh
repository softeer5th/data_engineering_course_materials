#!/bin/bash

# 역할에 따른 데몬 시작
if [ "$HADOOP_ROLE" == "master" ]; then
    echo "Starting master..."

    # HDFS 초기화 (최초 실행 시)
    if [ ! -d $HADOOP_HOME/hdfs/namenode/current ]; then
        echo "Formatting HDFS for the first time..."
        hdfs namenode -format
    fi
    echo "worker-1" >> $HADOOP_CONF_DIR/workers
    echo "worker-2" >> $HADOOP_CONF_DIR/workers
    echo "worker-3" >> $HADOOP_CONF_DIR/workers

    hdfs --daemon start namenode
    yarn --daemon start resourcemanager
    # $HADOOP_HOME/sbin/start-dfs.sh

elif [ "$HADOOP_ROLE" == "worker" ]; then
    echo "Starting worker..."

    hdfs --daemon start datanode
    yarn --daemon start nodemanager

else
    echo "Invalid HADOOP_ROLE: $HADOOP_ROLE"
    exit 1
fi