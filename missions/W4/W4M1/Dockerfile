FROM python:3.9-bullseye

# 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \  
    vim \                      
    ssh \                      
    python3-pip \              
    curl \
    wget \
    tar \
    bash && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install pandas numpy pyspark pyarrow

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV SPARK_VERSION=3.4.4
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# 로컬에서 제공되는 Spark 바이너리 파일 복사 및 설치
COPY spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

# Spark 작업 실행을 위한 스크립트 복사
COPY start-spark.sh /start-spark.sh
RUN chmod +x /start-spark.sh

# Spark 마스터 및 웹 UI 포트 노출
EXPOSE 8080 7077 4040 8888 8081

# 컨테이너 실행 시 start-spark.sh 실행
CMD ["/bin/bash", "/start-spark.sh"]