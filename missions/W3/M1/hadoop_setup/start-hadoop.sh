#!/bin/bash
set -e

service ssh start

# HDFS 포맷 (최초 실행 시에만 필요)
if [ ! -d "/hadoop_data/dfs/name/current" ]; then
    hdfs namenode -format
fi

# Hadoop 서비스 시작
start-dfs.sh
start-yarn.sh

# 컨테이너가 계속 실행되도록 대기
tail -f /dev/null