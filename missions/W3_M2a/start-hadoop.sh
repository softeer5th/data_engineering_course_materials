#!/bin/bash

# Format HDFS only on the master node
if [ "$HOSTNAME" == "master" ]; then
  if [ ! -d "/usr/local/hadoop/data/namenode" ]; then
    $HADOOP_HOME/bin/hdfs namenode -format -force
  fi
  $HADOOP_HOME/sbin/start-dfs.sh
  $HADOOP_HOME/sbin/start-yarn.sh
else
  $HADOOP_HOME/bin/hdfs datanode
  
  #아래로 바꿔서도 해보기
  # Start DataNode and NodeManager on worker nodes
  #$HADOOP_HOME/bin/hdfs datanode &
  #$HADOOP_HOME/bin/yarn nodemanager &
fi

# Keep container running
tail -f /dev/null