FROM eclipse-temurin:11-jre

# ARG 설정
ARG SPARK_VERSION=3.5.4
ARG SPARK_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
ARG SPARK_SHA512_URL="${SPARK_URL}.sha512"
ARG SPARK_HOME=/usr/local/spark

# ENV 설정
ENV SPARK_VERSION=${SPARK_VERSION} \
    SPARK_HOME=${SPARK_HOME} \
    PATH=${SPARK_HOME}/bin:/usr/bin/python:${PATH} \
    PYTHONPATH=${SPARK_HOME}/python/lib:$PYTHONPATH \
    PIP_BREAK_SYSTEM_PACKAGES=1

RUN sed -i 's|http://archive.ubuntu.com|http://mirror.kakao.com|g' /etc/apt/sources.list \
    && sed -i 's|http://security.ubuntu.com|http://mirror.kakao.com|g' /etc/apt/sources.list \
    && sed -i 's|http://ports.ubuntu.com|http://mirror.kakao.com|g' /etc/apt/sources.list \
    && apt-get update

# 시스템 패키지 설치
RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        curl \
        gpg \
        sudo \
        ssh \
        neovim && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# 스파크 다운로드 및 설치 & 무결성 검사
RUN cd /opt && \
    curl -fL "${SPARK_URL}" -o spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    curl -fL "${SPARK_SHA512_URL}" -o spark-${SPARK_VERSION}-bin-hadoop3.tgz.sha512 && \
    sha512sum -c spark-${SPARK_VERSION}-bin-hadoop3.tgz.sha512 && \
    tar xf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz spark-${SPARK_VERSION}-bin-hadoop3.tgz.sha512

RUN mkdir -p /tmp/spark-events

COPY config/ ${SPARK_HOME}/conf/

CMD ["/bin/bash"]