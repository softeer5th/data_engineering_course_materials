FROM ubuntu:20.04

# 환경 변수 설정
ENV HADOOP_VERSION=3.4.1
ENV HADOOP_HOME=/opt/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# 필수 패키지 설치
RUN apt-get update && \
    apt-get install -y wget vim openjdk-8-jdk curl openssh-client && \
    apt-get clean

# Hadoop 압축 파일 복사 및 설치
COPY hadoop-${HADOOP_VERSION}.tar.gz /tmp/
RUN tar -xzf /tmp/hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm -f /tmp/hadoop-${HADOOP_VERSION}.tar.gz

# HDFS용 사용자 추가 및 디렉토리 구성
RUN useradd -m -s /bin/bash hdfs && \
    mkdir -p /opt/hadoop_data/hdfs/namenode /opt/hadoop_data/hdfs/datanode && \
    chown -R hdfs:hdfs /opt/hadoop /opt/hadoop_data

# Hadoop 환경 파일 업데이트
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_USER=hdfs" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_DATANODE_USER=hdfs" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=hdfs" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

# Hadoop 설정 파일 복사
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml

# 초기화 스크립트 복사 및 실행
COPY src/init.sh /opt/hadoop/init.sh
RUN chmod +x /opt/hadoop/init.sh

# 사용자 권한 변경
USER hdfs

EXPOSE 22 9870 9000 9864

# 컨테이너 시작 시 초기화 스크립트 실행
ENTRYPOINT ["/opt/hadoop/init.sh"]
