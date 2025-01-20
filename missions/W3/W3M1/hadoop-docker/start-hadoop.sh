#!/bin/bash

# Set Hadoop user environment variables
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

# Start SSH service
service ssh start

# Start Hadoop services
start-dfs.sh
start-yarn.sh

# Keep the container running
tail -f /dev/null