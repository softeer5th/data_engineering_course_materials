FROM ubuntu:20.04

ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

RUN apt-get update && \
    apt-get install -y openjdk-8-jdk ssh rsync vim && \
    apt-get clean

COPY ./hadoop-$HADOOP_VERSION.tar.gz /hadoop-$HADOOP_VERSION.tar.gz
RUN tar -xzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

COPY ./config/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY ./config/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY ./config/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
COPY ./config/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml

RUN ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN echo "hadoop-worker1\nhadoop-worker2\nhadoop-worker3" > $HADOOP_HOME/etc/hadoop/workers

CMD service ssh start && tail -f /dev/null
