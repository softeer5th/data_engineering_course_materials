# 베이스 이미지 선택 (우분투)
FROM ubuntu:20.04

# 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk wget curl net-tools ssh \
    && apt-get clean

# Hadoop 다운로드 및 설치
# 빠른 다운로드 링크로 변경 (가까운 Apache Mirror 사용)
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /usr/local/hadoop && \
    rm hadoop-3.3.6.tar.gz

# Spark 다운로드 및 설치
# 빠른 다운로드 링크로 변경 (가까운 Apache Mirror 사용)
RUN wget https://dlcdn.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.4-bin-hadoop3.tgz && \
    mv spark-3.5.4-bin-hadoop3 /usr/local/spark && \
    rm spark-3.5.4-bin-hadoop3.tgz


# 환경 변수 설정
ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
ENV HADOOP_HOME=/usr/local/hadoop
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin


# core-site.xml과 hdfs-site.xml 복사 (HDFS 환경 설정)
COPY core-site.xml $HADOOP_HOME/etc/hadoop/
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/

# 환경 변수 설정
ENV HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root

# Hadoop 데이터 디렉토리 생성 및 초기화
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data && \
    chown -R root:root /hadoop/dfs && \
    $HADOOP_HOME/bin/hdfs namenode -format

# Spark 환경 설정 복사
# COPY spark-defaults.conf $SPARK_HOME/conf/
COPY start-spark.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/start-spark.sh

# SSH 설정 (클러스터 내 통신용)
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Hadoop 데이터 디렉토리 생성 및 초기화
# RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data && \
#     chown -R root:root /hadoop/dfs && \
#     $HADOOP_HOME/bin/hdfs namenode -format

# 작업 디렉토리 설정
# WORKDIR $HADOOP_HOME

# 컨테이너 시작 시 실행할 스크립트 복사
# COPY start-hadoop.sh /usr/local/bin/start-hadoop.sh
# RUN chmod +x /usr/local/bin/start-hadoop.sh

# 스크립트를 컨테이너에 복사
COPY submit-pi-and-check.sh /usr/local/bin/submit-pi-and-check.sh
RUN chmod +x /usr/local/bin/submit-pi-and-check.sh

COPY pi_with_output.py /opt/spark-data/pi_with_output.py
RUN chmod +x /opt/spark-data/pi_with_output.py

# 포트 공개
# HDFS 및 Spark에서 사용하는 포트 설정
EXPOSE 9870 9864 8080 7077 8081

# 작업 디렉토리 설정
WORKDIR $SPARK_HOME

# Hadoop 환경설정 파일에 JAVA_HOME 환경변수 추가
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# 컨테이너 시작 시 스크립트 실행
CMD ["/usr/local/bin/start-spark.sh"]