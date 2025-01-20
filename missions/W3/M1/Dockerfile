# ubuntu-20.04 base.
FROM eclipse-temurin:8-jdk-focal

# 필수 패키지 설치
# DEBIAN_FRONTEND=noninteractive: apt-get install 시 interactive 설정을 무시하고 설치 진행 (모두 yes로 설정)
RUN apt-get update -y \
    && export DEBIAN_FRONTEND=noninteractive && apt-get install -y --no-install-recommends \
        sudo \
        curl \
        ssh \
    && apt-get clean
# 계정 생성과 동시에 홈 디렉토리에 생성
# hduser 계정 및 패스워드 설정
# 파이프라인 |을 사용하여 바로 useradd 명령어의 인수로 전달
# hduser 계정에 sudo 권한 부여
# hduser가 명령 수행 시 패스워드를 물어보지 않도록 설정 (/etc/sudoers 파일에 hduser 계정에 대한 NOPASSWD 설정 추가)
# /usr/bin 디렉토리에 python3 바이너리를 python으로 링크
RUN useradd -m hduser && echo "hduser:supergroup" | chpasswd && adduser hduser sudo && echo "hduser     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers && cd /usr/bin/ && sudo ln -s python3 python
WORKDIR /app
COPY . /app
COPY missions/W3/hadoop-single-node-cluster/ssh_config /etc/ssh/ssh_config

WORKDIR /home/hduser
USER hduser
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 0600 ~/.ssh/authorized_keys
ENV HADOOP_VERSION=3.3.3
ENV HADOOP_HOME=/home/hduser/hadoop-${HADOOP_VERSION}

RUN curl -sL --retry 3 \
  "http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
  | gunzip \
  | tar -x -C /home/hduser/ \
 && rm -rf ${HADOOP_HOME}/share/doc

ENV HDFS_NAMENODE_USER hduser
ENV HDFS_DATANODE_USER hduser
ENV HDFS_SECONDARYNAMENODE_USER hduser

ENV YARN_RESOURCEMANAGER_USER hduser
ENV YARN_NODEMANAGER_USER hduser

RUN echo "export JAVA_HOME=/opt/java/openjdk/" >> $HADOOP_HOME/etc/hadoop/hadoop-export.sh
COPY missions/W3/hadoop-single-node-cluster/core-site.xml $HADOOP_HOME/etc/hadoop/
COPY missions/W3/hadoop-single-node-cluster/hdfs-site.xml $HADOOP_HOME/etc/hadoop/
COPY missions/W3/hadoop-single-node-cluster/yarn-site.xml $HADOOP_HOME/etc/hadoop/

COPY missions/W3/hadoop-single-node-cluster/docker-entrypoint.sh $HADOOP_HOME/etc/hadoop/

ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

ADD examples/ examples/ 

EXPOSE 50070 50075 50010 50020 50090 8020 9000 9864 9870 10020 19888 8088 8030 8031 8032 8033 8040 8042 22

WORKDIR /usr/local/bin
# RUN sudo ln -s ${HADOOP_HOME}/etc/hadoop/docker-entrypoint.sh .
# WORKDIR /home/hduser

# YARNSTART=0 will prevent yarn scheduler from being launched
ENV YARNSTART 0