FROM ubuntu:24.04

# Set environment variables for Hadoop
ENV DEBIAN_FRONTEND = noninteratcive


# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    net-tools \
    ssh \
    rsync \
    vim \
    sudo \
    openssh-server \
    openssh-client \
    && apt-get clean 

RUN apt-get install -y openjdk-11-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=${PATH}:${JAVA_HOME}/bin

# install python and pyspark
RUN apt-get update && \
    apt-get install -y python3 python3-pip
# RUN pip3 install pyspark

# Setting Hadoop ENVs
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_COMMON_LIB_NATIVE_DIR=/usr/local/hadoop/lib/native
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin

# Download and install Hadoop
RUN wget https://mirrors.sonic.net/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar xvzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Hadoop configuration files
COPY config/*.xml /usr/local/hadoop/etc/hadoop/

# Set up SSH and ensure /run/sshd exists
RUN mkdir -p /var/run/sshd && chmod 0755 /var/run/sshd

# Add JAVA_HOME to hadoop-env.sh
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh



# USER Settings: 루트 유저와 spark 유저를 분리
RUN echo 'root:root' | chpasswd
## make home dir for sparkuser and set Bash as base shell, set passwd, grant sudo permission
RUN useradd -m -s /bin/bash sparkuser && \
    echo "sparkuser:sparkuser" | chpasswd && \
    adduser sparkuser sudo

# Do not request password to sparkuser
RUN echo "sparkuser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Configure SSH for passwordless communication
# /.ssh <-- for communicate without passwd
# -t ras : key_type=RSA, -P '' : no passwd, -f <dir> : save key to <dir>
# add public key to authorized_keys -> allow sparkuser to access without passwd
RUN mkdir /home/sparkuser/.ssh && \
    chmod 700 /home/sparkuser/.ssh
RUN ssh-keygen -t rsa -P '' -f /home/sparkuser/.ssh/id_rsa 
RUN cat /home/sparkuser/.ssh/id_rsa.pub >> /home/sparkuser/.ssh/authorized_keys && \
    chmod 600 /home/sparkuser/.ssh/authorized_keys && \
    chown -R sparkuser:sparkuser /home/sparkuser/.ssh

# SSH 구성 설정: 루트 로그인 허용 및 패스워드 인증 활성화
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config
RUN echo "AllowUsers sparkuser" >> /etc/ssh/sshd_config


# also change ownership of hadoop dir and files to sparkuser
RUN chown -R sparkuser:sparkuser /usr/local/hadoop

# To Avoid "WARN util.NativeCodeLoader: ~~~ " Warning : But it doesn't work
ENV HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib/native"
# ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${HADOOP_HOME}/lib/native
ENV LD_LIBRARY_PATH=${HADOOP_HOME}/lib/native

# Expose HDFS and YARN
EXPOSE 22 9870 8088 9000 9864 9866 8020 8080


# ------ Install Spark ------
RUN mkdir /temp
COPY spark-3.5.4-bin-hadoop3.tgz /temp/

RUN tar -xvf /temp/spark-3.5.4-bin-hadoop3.tgz -C /usr/local/ && \
    mv /usr/local/spark-3.5.4-bin-hadoop3 /usr/local/spark && \
    rm -rf /temp

ENV SPARK_HOME=/usr/local/spark
ENV PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# sparkuser 생성 및 환경 변수 적용
RUN usermod -aG sudo sparkuser && \
    echo 'export SPARK_HOME=/usr/local/spark' >> /home/sparkuser/.bashrc && \
    echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> /home/sparkuser/.bashrc && \
    mkdir -p /home/sparkuser && \
    chown -R sparkuser:sparkuser /home/sparkuser /usr/local/spark





# Add entrypoint and terminate script
# COPY start-spark.sh /usr/local/bin/start-spark.sh
COPY start-spark.sh /home/sparkuser/start-spark.sh

# RUN chmod +x /usr/local/bin/start-spark.sh
RUN chmod +x /home/sparkuser/start-spark.sh

USER sparkuser
WORKDIR /home/sparkuser

# Set entrypoint script
ENTRYPOINT [ "/bin/bash", "-c", "./start-spark.sh" ]
