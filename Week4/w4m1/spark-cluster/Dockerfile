FROM openjdk:11

ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

RUN apt-get update && apt-get install -y \
    curl tar python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*  # 패키지 캐시 제거 (이미지 크기 최적화)

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

WORKDIR /opt/spark

EXPOSE 8080 7077 8081

COPY scripts/start-master.sh /start-master.sh
COPY scripts/start-worker.sh /start-worker.sh
RUN chmod +x /start-master.sh /start-worker.sh

RUN mkdir -p /opt/spark/output

CMD ["/bin/bash"]
