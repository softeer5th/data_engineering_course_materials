#!/bin/bash

# Start SSH service
service ssh start

# Format the Hadoop namenode
$HADOOP_HOME/bin/hdfs namenode -format

# Start Hadoop DFS and YARN services
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Keep the container running
exec bash