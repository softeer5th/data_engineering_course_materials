FROM ubuntu:20.04

# 변수 설정 (docker-compose.yml에서 변경 가능)
ARG HADOOP_DOWNLOAD_URL=https://dlcdn.apache.org/hadoop/common
ARG HADOOP_VERSION=3.4.1
ARG HADOOP_HOME=/usr/local/hadoop

# apt 패키지 소스 변경
RUN arch=$(dpkg --print-architecture) && \
    case "$arch" in \
    "amd64") sed -i 's/archive.ubuntu.com/mirror.kakao.com/g' /etc/apt/sources.list \
        && sed -i 's/security.ubuntu.com/mirror.kakao.com/g' /etc/apt/sources.list ;; \
    "arm64") sed -i 's|ports.ubuntu.com|ftp.kaist.ac.kr/ubuntu-ports|g' /etc/apt/sources.list ;; \
    esac

# 필수 패키지 설치
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        openjdk-8-jdk-headless \
        python3-pip \
        curl \
        ssh \
        rsync \
        sudo && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 하둡 다운로드 및 설치
RUN curl -O https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzvf hadoop-${HADOOP_VERSION}.tar.gz && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME}