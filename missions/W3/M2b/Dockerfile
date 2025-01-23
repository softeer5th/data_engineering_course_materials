
FROM ubuntu:20.04

ENV HADOOP_VERSION=3.3.6-aarch64
ENV HADOOP_HOME=/usr/local/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV PATH="/usr/bin/python3.9:$PATH"


RUN apt-get update && \
    apt-get install -y openjdk-11-jdk curl rsync sudo vim && \
    apt install python3.9 -y && \
    apt-get clean

RUN curl -O https://mirrors.sonic.net/apache/hadoop/common/hadoop-3.3.6/hadoop-3.3.6-aarch64.tar.gz && \
    tar -xzvf hadoop-3.3.6-aarch64.tar.gz && \
    mv hadoop-3.3.6 /usr/local/hadoop && \
    rm hadoop-3.3.6-aarch64.tar.gz

COPY config/*.xml /usr/local/hadoop/etc/hadoop/
# Add JAVA_HOME to hadoop-env.sh
RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

EXPOSE 9870 9864 8088 9000 8042

CMD ["/bin/bash"]