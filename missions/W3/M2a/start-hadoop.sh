#!/bin/bash

# SSH 서비스 시작
service ssh start

# Master 노드에서만 NameNode 및 ResourceManager 실행
if [[ $HOSTNAME == "master" ]]; then
    # $HADOOP_HOME/bin/hdfs namenode -format -force || true
    $HADOOP_HOME/sbin/start-dfs.sh
    $HADOOP_HOME/sbin/start-yarn.sh
    tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log
fi

# Worker 노드는 DataNode 및 NodeManager 실행
if [[ $HOSTNAME != "master" ]]; then
    $HADOOP_HOME/bin/hdfs --daemon start datanode
    $HADOOP_HOME/bin/yarn --daemon start nodemanager
    tail -f $HADOOP_HOME/logs/hadoop-*-datanode-*.log
fi

# 컨테이너 실행 유지
#tail -f /dev/null