FROM openjdk:8-jdk-slim

ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3

RUN apt-get update && \
    apt-get install -y curl python3 python3-pip && \
    apt-get clean

COPY spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tar /tmp/
RUN tar -xvf /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tar -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm -f /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tar

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

WORKDIR /opt/spark

CMD ["spark-class", "org.apache.spark.deploy.master.Master"]