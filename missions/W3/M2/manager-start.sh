#!/bin/bash
set -e

service ssh start

sed -i "s#\${HADOOP_DATA}#$HADOOP_DATA#g" $HADOOP_HOME/etc/hadoop/hdfs-site.xml

# NameNode HDFS 디렉터리
dfs_namenode_name_dir=$(hdfs getconf -confKey dfs.namenode.name.dir)
dfs_namenode_name_dir=${dfs_namenode_name_dir#file://}

# HDFS NameNode 포맷 (최초 1회만)
if [ ! -d "$dfs_namenode_name_dir/current" ]; then
  echo "Formatting HDFS NameNode..."
  hdfs namenode -format
fi

hdfs --daemon start namenode
yarn --daemon start resourcemanager

tail -f /dev/null