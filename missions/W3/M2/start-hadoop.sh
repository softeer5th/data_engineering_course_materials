#!/bin/bash
set -e

service ssh start

# NameNode HDFS 디렉터리
dfs_namenode_name_dir=$(hdfs getconf -confKey dfs.namenode.name.dir)
# file:// 프로토콜 제거
dfs_namenode_name_dir=${dfs_namenode_name_dir#file://}

# HDFS NameNode 포맷 (최초 1회만)
if [ ! -d "$dfs_namenode_name_dir/current" ]; then
  echo "Formatting HDFS NameNode..."
  hdfs namenode -format
fi

# Hadoop 서비스 시작
start-dfs.sh
start-yarn.sh

# 컨테이너가 계속 실행되도록 대기
tail -f /dev/null