# Base Image: Use an appropriate Linux distribution
FROM ubuntu:24.04

USER root

EXPOSE 22

# Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    openssh-server \
    rsync \
    sudo \
    && apt-get clean

# Create users for HDFS and YARN
RUN groupadd hadoop && \
    useradd -d /home/hadoop -m -g hadoop hadoop && \
    echo "hadoop ALL=(hadoop) NOPASSWD: /usr/sbin/service ssh start" >> /etc/sudoers

# SSH 서버 설정 (sshd_config 수정)
RUN sed -i 's/^#Port 22/Port 22/' /etc/ssh/sshd_config && \
    sed -i 's/^#PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config && \
    sed -i 's/^#PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config && \
    echo "PermitEmptyPasswords no" >> /etc/ssh/sshd_config

# SSH 폴더 및 권한 설정
RUN mkdir -p /home/hadoop/.ssh && \
    chmod 700 /home/hadoop/.ssh && \
    ssh-keygen -t ed25519 -f /home/hadoop/.ssh/id_ed25519 -P "" && \
    cat /home/hadoop/.ssh/id_ed25519.pub >> /home/hadoop/.ssh/authorized_keys && \
    chmod 600 /home/hadoop/.ssh/authorized_keys && \
    chown -R hadoop:hadoop /home/hadoop/.ssh
    
# Set environment variables for Hadoop
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin

RUN echo 'export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH' >> /home/hadoop/.bashrc
RUN echo 'export JAVA_HOME=$JAVA_HOME' >> /home/hadoop/.bashrc
RUN echo 'export HADOOP_HOME=$HADOOP_HOME' >> /home/hadoop/.bashrc

# Download and install Hadoop
RUN wget https://mirrors.sonic.net/apache/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar xvzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# set hadoop-env.sh
RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export HADOOP_HOME=$HADOOP_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export HADOOP_CONF_DIR=$HADOOP_CONF_DIR" >> $HADOOP_CONF_DIR/hadoop-env.sh

RUN echo "export HDFS_NAMENODE_USER=hadoop" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export HDFS_DATANODE_USER=hadoop" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export HDFS_SECONDARYNAMENODE_USER=hadoop" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export YARN_RESOURCEMANAGER_USER=hadoop" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export YARN_NODEMANAGER_USER=hadoop" >> $HADOOP_CONF_DIR/hadoop-env.sh

# Hadoop configuration files
COPY conf/*.xml $HADOOP_CONF_DIR
COPY conf/workers $HADOOP_CONF_DIR

# Configure permissions for Hadoop directory
RUN chown -R hadoop:hadoop $HADOOP_HOME && \
    chmod -R 775 $HADOOP_HOME

COPY hadoop/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set entrypoint script
ENTRYPOINT ["entrypoint.sh"]