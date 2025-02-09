FROM openjdk:11-jdk-slim
ENV SPARK_VERSION=3.4.4

RUN apt-get update && \
    apt-get install -y curl && \
    curl -o spark.tgz https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xvf spark.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN apt-get install -y python3 python3-pip
WORKDIR /opt/spark
EXPOSE 8080 7077 4040