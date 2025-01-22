#!/bin/bash
# namenode-entrypoint.sh
# 에러 발생 시 즉시 중단
set -e
# ssh 서비스 시작
sudo service ssh start

sudo mkdir -p /hadoop/dfs/name
sudo chown -R hduser:hduser /hadoop && \
sudo chmod -R 755 /hadoop

# namenode 디렉토리의 버젼이 존재하지 않으면 namenode를 포맷
if [ ! -f "/hadoop/dfs/name/current/VERSION" ]; then
    $HADOOP_HOME/bin/hdfs namenode -format
    echo "Successfully formatted namenode"

    # HDFS에 기본 디렉토리 생성
        $HADOOP_HOME/bin/hdfs dfs -mkdir /tmp
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p /users/hduser
        $HADOOP_HOME/bin/hdfs dfs -mkdir /jars
        
        # 생성된 디렉토리들에 모든 권한 (rwx) 부여
        $HADOOP_HOME/bin/hdfs dfs -chmod 777 /tmp
        $HADOOP_HOME/bin/hdfs dfs -chmod 777 /users
        $HADOOP_HOME/bin/hdfs dfs -chmod 777 /jars

fi

# worker 파일 복사
# workers 파일에 혹시 모를 localhost 제거를 위함
echo "datanode1" > $HADOOP_HOME/etc/hadoop/workers && \
echo "datanode2" >> $HADOOP_HOME/etc/hadoop/workers

# 서비스 시작 전 대기
sleep 10

# HDFS 시작
$HADOOP_HOME/sbin/start-dfs.sh
# yarn 시작
if [[ -z $YARNSTART || $YARNSTART -ne 0 ]]; then
        echo "running start-yarn.sh"
        $HADOOP_HOME/sbin/start-yarn.sh
fi

# NameNode가 시작될 때까지 대기
until hdfs dfsadmin -report > /dev/null 2>&1
do
    echo "Waiting for NameNode to start..."
    sleep 5
done


echo "Current JAVA_HOME: $JAVA_HOME"
echo "Java version: $(java -version)"

# HDFS SafeMode 해제
$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave

# namenode 로그를 계속 모니터링하면서 컨테이너 실행 유지
tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log
# 계속 실행
# tail -f /dev/null