#!/bin/bash
set -e

echo "Stopping YARN ${HADOOP_YARN_ROLE}..."
yarn --daemon stop ${HADOOP_YARN_ROLE}

echo "Stopping HDFS ${HADOOP_HDFS_ROLE}..."
hdfs --daemon stop ${HADOOP_HDFS_ROLE}

echo "Hadoop services stopped."