FROM ubuntu:20.04

ENV HADOOP_VERSION=3.3.6-aarch64
ENV HADOOP_HOME=/usr/local/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk curl rsync sudo vim python3-pip openssh-server && \
    ln -sf /usr/bin/python3.8 /usr/bin/python && \
    pip install --upgrade pip && \
    apt-get clean

RUN curl -O https://mirrors.sonic.net/apache/hadoop/common/hadoop-3.3.6/hadoop-3.3.6-aarch64.tar.gz && \
    tar -xzvf hadoop-3.3.6-aarch64.tar.gz && \
    mv hadoop-3.3.6 /usr/local/hadoop && \
    rm hadoop-3.3.6-aarch64.tar.gz


ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
COPY config/*.xml $HADOOP_CONF_DIR/
COPY start-system.sh /
RUN chmod +x /start-system.sh

RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

EXPOSE 9870 9864 8088 9000 8042

CMD ["/bin/bash"]
