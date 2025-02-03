FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    tar \
    vim \
    python3-pip \
    pipx \
    && apt-get clean

ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN wget -nv -O spark-${SPARK_VERSION}.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvzf spark-${SPARK_VERSION}.tgz && \
    mv /spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/ ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}.tgz

ENV SPARK_MASTER=spark://spark-master:7077
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077

RUN mkdir -p ${SPARK_HOME}/spark-events

RUN groupadd spark && \
    useradd -m -g spark -d /home/spark spark && \
    chown -R spark:spark ${SPARK_HOME} && \
    chmod -R 775 ${SPARK_HOME}

USER spark

COPY conf/* ${SPARK_HOME}/conf

RUN pipx install pyspark
RUN mkdir -p /home/spark/spark_input
RUN mkdir -p /home/spark/spark_output

EXPOSE 7077 8080 8081

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]