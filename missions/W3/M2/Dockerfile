FROM ubuntu:20.04

# 하둡 다운로드 URL 및 버전
ARG HADOOP_DOWNLOAD_URL=https://dlcdn.apache.org/hadoop/common
ARG HADOOP_VERSION=3.4.1

# 환경 변수 설정
ENV HADOOP_HOME=${HADOOP_HOME_ARG}
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HADOOP_DATA=${HADOOP_DATA_ARG}

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul

# apt 패키지 소스 변경
RUN arch=$(dpkg --print-architecture) && \
    case "$arch" in \
        "amd64") sed -i 's/archive.ubuntu.com/mirror.kakao.com/g' /etc/apt/sources.list \
                 && sed -i 's/security.ubuntu.com/mirror.kakao.com/g' /etc/apt/sources.list ;; \
        "arm64") sed -i 's|ports.ubuntu.com|ftp.kaist.ac.kr/ubuntu-ports|g' /etc/apt/sources.list ;; \
    esac

# 필수 패키지 설치
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk-headless curl ssh rsync sudo && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 하둡 다운로드 및 설치
RUN curl -O ${HADOOP_DOWNLOAD_URL}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

RUN tar -xzvf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# hadoop 유저 생성 및 권한 설정
RUN groupadd hadoop && \
    useradd -m -s /bin/bash -g hadoop hadoop && \
    echo "hadoop ALL=(ALL) NOPASSWD: /usr/sbin/useradd, /usr/sbin/usermod, /usr/bin/passwd" >> /etc/sudoers

RUN mkdir -p $HADOOP_DATA

RUN chown -R hadoop:hadoop $HADOOP_HOME
RUN chown -R hadoop:hadoop $HADOOP_DATA

# hadoop 유저로 전환
USER hadoop

WORKDIR /home/hadoop

# 하둡 설정 파일 복사
COPY --chown=hadoop:hadoop config/ $HADOOP_HOME/etc/hadoop/

# start-hadoop.sh 복사
COPY --chown=hadoop:hadoop start-hadoop.sh /start-hadoop.sh
RUN chmod +x /start-hadoop.sh

# 포트 노출
EXPOSE 9870 9864 8088 9000

# 컨테이너 실행 시 start-hadoop.sh 실행
CMD ["/start-hadoop.sh"]
