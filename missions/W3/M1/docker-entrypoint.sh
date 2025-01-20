#!/bin/bash
# 에러 발생 시 즉시 중단
set -e
# ssh 서비스 시작
sudo service ssh start

# namenode 디렉토리가 없으면 포맷 실행
if [ ! -d "/tmp/hadoop-hduser/dfs/name" ]; then
        $HADOOP_HOME/bin/hdfs namenode -format && echo "OK : HDFS namenode format operation finished successfully !"
fi

# HDFS 시작
$HADOOP_HOME/sbin/start-dfs.sh

echo "YARNSTART = $YARNSTART"

# YARNSTART가 설정되지 않았거나 0이 아니면 yarn도 시작.
if [[ -z $YARNSTART || $YARNSTART -ne 0 ]]; then
        echo "running start-yarn.sh"
        $HADOOP_HOME/sbin/start-yarn.sh
fi

# HDFS에 기본 디렉토리 생성
$HADOOP_HOME/bin/hdfs dfs -mkdir /tmp
$HADOOP_HOME/bin/hdfs dfs -mkdir /users
$HADOOP_HOME/bin/hdfs dfs -mkdir /jars

# 생성된 디렉토리들에 모든 권한 (rwx) 부여
$HADOOP_HOME/bin/hdfs dfs -chmod 777 /tmp
$HADOOP_HOME/bin/hdfs dfs -chmod 777 /users
$HADOOP_HOME/bin/hdfs dfs -chmod 777 /jars

# HDFS SafeMode 해제
$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave

# namenode 로그를 계속 모니터링하면서 컨테이너 실행 유지
tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log
