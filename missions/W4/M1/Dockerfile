FROM ubuntu:20.04

ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV DEBIAN_FRONTEND=noninteractive

RUN sed -i 's|http://archive.ubuntu.com/ubuntu/|http://ftp.kaist.ac.kr/ubuntu/|g' /etc/apt/sources.list && \
    sed -i 's|http://ports.ubuntu.com/ubuntu-ports|http://ftp.kaist.ac.kr/ubuntu-ports|g' /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y \
        openjdk-8-jdk \
        curl \
        wget \
        python3-pip \
        net-tools && \
    apt-get clean && rm -rf /var/lib/apt/lists/*



RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN pip3 install pyspark

WORKDIR /opt/spark

CMD ["/bin/bash"]
