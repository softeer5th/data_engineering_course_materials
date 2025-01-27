# 베이스 이미지 선택 (우분투)
FROM ubuntu:20.04

# 필요한 패키지 업데이트 및 설치
RUN apt-get update && apt-get install -y \
    wget ssh openjdk-8-jdk sudo && apt-get clean

# Hadoop 설치
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz && \
    tar -xzvf hadoop-3.4.1.tar.gz && \
    mv hadoop-3.4.1 /usr/local/hadoop && \
    rm hadoop-3.4.1.tar.gz

# 환경 변수 설정(하둡 버전, 하둡 경로, 자바 경로, path설정)
ENV HADOOP_VERSION=3.4.1 \
    HADOOP_HOME=/usr/local/hadoop \
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64 \
    PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin \
    HADOOP_MAPRED_HOME=/usr/local/hadoop

# 환경 변수 설정
ENV HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root

# SSH 설정 (Hadoop은 SSH로 내부 통신을 함)
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Hadoop 환경 설정
COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
COPY yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml

# Hadoop 데이터 디렉토리 생성 및 초기화
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data && \
    chown -R root:root /hadoop/dfs && \
    $HADOOP_HOME/bin/hdfs namenode -format

# 작업 디렉토리 설정
WORKDIR $HADOOP_HOME

# 컨테이너 시작 시 실행할 스크립트 복사
COPY start-hadoop.sh /usr/local/bin/start-hadoop.sh
RUN chmod +x /usr/local/bin/start-hadoop.sh

# Hadoop 포트 공개
EXPOSE 9870 9864 8088 8042 22

# Hadoop 환경설정 파일에 JAVA_HOME 환경변수 추가
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# mapper/ reducer 실행을 위한 코드
COPY mapper.py /usr/local/hadoop/mapper.py
COPY reducer.py /usr/local/hadoop/reducer.py
# 실행 권한 추가
RUN chmod +x /usr/local/hadoop/mapper.py
RUN chmod +x /usr/local/hadoop/reducer.py

# 감정분석을 위한 트위터 데이터셋
COPY training.1600000.processed.noemoticon.csv /app/training.1600000.processed.noemoticon.csv


# 컨테이너 시작 시 스크립트 실행
CMD ["start-hadoop.sh"]