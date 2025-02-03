FROM openjdk:11

# 환경 변수 설정 (Spark, Hadoop 버전)
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# 필수 패키지 설치 (curl, tar, Python3)
RUN apt-get update && apt-get install -y \
    curl tar python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*  # 패키지 캐시 제거 (이미지 크기 최적화)

# Spark 다운로드 및 설치
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# 작업 디렉토리 설정
WORKDIR /opt/spark

# Spark Web UI 및 Cluster 포트 노출
EXPOSE 8080 7077 8081

# Spark 실행 스크립트 복사
COPY scripts/start-master.sh /start-master.sh
COPY scripts/start-worker.sh /start-worker.sh
RUN chmod +x /start-master.sh /start-worker.sh

# Output 디렉토리 생성 (결과 저장용)
RUN mkdir -p /opt/spark/output

# 기본 실행 명령
CMD ["/bin/bash"]
