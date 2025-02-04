#!/bin/bash

# 필수 패키지 설치
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends sudo curl ssh
apt-get clean

# hduser 계정 생성 및 설정
useradd -m hduser
echo "hduser:supergroup" | chpasswd
adduser hduser sudo
echo "hduser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Python 심볼릭 링크 생성 (먼저 python3 설치)
apt-get install -y python3
cd /usr/bin/
ln -sf python3 python

# SSH 설정 (ssh_config 파일이 현재 디렉토리에 있다고 가정)
cp /app/missions/W3/hadoop-single-node-cluster/ssh_config /etc/ssh/ssh_config

# hduser로 전환하여 작업 수행
# su - hduser << 'EOF'

# SSH 키 생성
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# Hadoop 설치
export HADOOP_VERSION=3.3.3
export HADOOP_HOME=/home/hduser/hadoop-${HADOOP_VERSION}

curl -sL --retry 3 \
  "http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
  | gunzip \
  | tar -x -C /home/hduser/
rm -rf ${HADOOP_HOME}/share/doc

# 환경 변수 설정. 네임노드, 데이터노드, 리소스매니저, 노드매니저 계정 모두 hduser로 설정
# secondarynamenode: fsimage와 Edit log를 복사하여 네임노드 다운 시 동안의 메타데이터 복구를 도움.
export HDFS_NAMENODE_USER=hduser

export HDFS_DATANODE_USER=hduser
export HDFS_SECONDARYNAMENODE_USER=hduser
export YARN_RESOURCEMANAGER_USER=hduser
export YARN_NODEMANAGER_USER=hduser

# Java 홈 설정 (Hadoop 환경에서)
echo "export JAVA_HOME=/opt/java/openjdk/" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Hadoop 설정 파일 복사 (파일들이 현재 디렉토리에 있다고 가정)
cp /app/missions/W3/hadoop-single-node-cluster/core-site.xml $HADOOP_HOME/etc/hadoop/
cp /app/missions/W3/hadoop-single-node-cluster/hdfs-site.xml $HADOOP_HOME/etc/hadoop/
cp /app/missions/W3/hadoop-single-node-cluster/yarn-site.xml $HADOOP_HOME/etc/hadoop/
cp /app/missions/W3/hadoop-single-node-cluster/docker-entrypoint.sh $HADOOP_HOME/etc/hadoop/

# PATH 설정
echo "export PATH=\$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin" >> ~/.bashrc

# docker-entrypoint.sh 심볼릭 링크 생성
sudo ln -s ${HADOOP_HOME}/etc/hadoop/docker-entrypoint.sh /usr/local/bin/

# YARN 시작 설정
export YARNSTART=0

# EOF

# 필요한 포트 목록 (참고용)
# 50070 50075 50010 50020 50090 8020 9000 9864 9870 10020 19888 8088 8030 8031 8032 8033 8040 8042 22