FROM ubuntu:20.04

# 하둡 버전 및 URL 설정
ARG HADOOP_DOWNLOAD_URL=https://dlcdn.apache.org/hadoop/common
ENV HADOOP_VERSION=3.4.1
ENV HADOOP_HOME=/usr/local/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul

RUN apt-get update

# JAVA 설치
RUN apt-get install -y openjdk-8-jdk-headless

# 기타 필수 패키지 설치
RUN apt-get install -y curl ssh rsync sudo && apt-get clean

# SSH 설정
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    chmod 700 ~/.ssh && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    echo "UserKnownHostsFile /dev/null" >> /etc/ssh/ssh_config

# 하둡 다운로드 및 설치
RUN curl -O ${HADOOP_DOWNLOAD_URL}/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzvf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# 하둡 설정 파일 복사
COPY config/ $HADOOP_HOME/etc/hadoop/

# start-hadoop.sh 복사
COPY start-hadoop.sh /start-hadoop.sh
RUN chmod +x /start-hadoop.sh

# 포트 노출
EXPOSE 9870 9864 8088 9000

# 컨테이너 실행 시 start-hadoop.sh 실행
CMD ["/start-hadoop.sh"]
