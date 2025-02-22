FROM ubuntu/hadoop:latest

RUN apt-get install -y \
    tar \
    python3-pip \
    pipx \
    && apt-get clean

ENV HADOOP_VERSION=3
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV SPARK_VERSION=3.5.4
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

COPY spark/spark-3.5.4-bin-hadoop3.tgz spark-$SPARK_VERSION.tgz
RUN tar xvzf spark-$SPARK_VERSION.tgz && \
    mv /spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION/ $SPARK_HOME && \
    rm spark-$SPARK_VERSION.tgz

RUN groupadd spark && \
    useradd -m -g spark -d /home/spark spark && \
    chown -R spark:spark $SPARK_HOME && \
    chmod -R 775 $SPARK_HOME

USER spark

RUN pipx install pyspark

COPY conf/*.xml $HADOOP_CONF_DIR
COPY conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

EXPOSE 7077 8080 4040 18080

CMD ["tail", "-f", "/dev/null"]