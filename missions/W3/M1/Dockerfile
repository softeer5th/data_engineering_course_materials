FROM eclipse-temurin:8-jdk-focal

# 환경 변수 설정
ENV HADOOP_VERSION=3.3.5
ENV HADOOP_HOME=/usr/local/hadoop
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# 패키지 설치
# curl : URL에서 파일이나 데이터를 다운로드 하는데 사용됨.
# rsync : 로컬 디렉토리 간 또는 네트워크를 통해 파일과 디렉토리를 효율적으로 동기화하는 도구
RUN apt-get update && \
    apt-get install -y curl rsync && \
    apt-get clean

# 하둡 파일 다운로드 및 설치    
RUN curl -O https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzvf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

RUN apt-get install -y ssh

ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root 

WORKDIR /app

COPY . /app

# 하둡 설정 파일 복사
COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
COPY yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml

# SSH키 생성 및 인증
# RSA키 생성
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

RUN echo 'export JAVA_HOME=/opt/java/openjdk' >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh

EXPOSE 9870 9864 8088 9000
RUN $HADOOP_HOME/bin/hdfs namenode -format 

# bash 셸에서 ssh 키고, 하둡 실행 명령, 실시간 로그
CMD ["/bin/bash", "-c", "service ssh start && $HADOOP_HOME/sbin/start-dfs.sh && tail -f $HADOOP_HOME/logs/*"]
