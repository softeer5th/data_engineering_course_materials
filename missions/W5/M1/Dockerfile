# ubuntu-20.04 base.
FROM ket0825/spark-base:latest

# 필수 패키지 설치
# DEBIAN_FRONTEND=noninteractive: apt-get install 시 interactive 설정을 무시하고 설치 진행 (모두 yes로 설정)
RUN sudo apt-get update -y \
    && export DEBIAN_FRONTEND=noninteractive \
    && sudo apt-get install -y --no-install-recommends \
        sudo \
        curl \
        ssh

# 계정 생성과 동시에 홈 디렉토리에 생성
# hduser 계정 및 패스워드 설정
# 파이프라인 |을 사용하여 바로 useradd 명령어의 인수로 전달
# hduser 계정에 sudo 권한 부여
# hduser가 명령 수행 시 패스워드를 물어보지 않도록 설정 (/etc/sudoers 파일에 hduser 계정에 대한 NOPASSWD 설정 추가)
# /usr/bin 디렉토리에 python3 바이너리를 python으로 링크

# 비밀번호 추가 시 설정
# RUN  echo "hduser:password" | chpasswd
# RUN useradd -m hduser && adduser hduser sudo && echo "hduser     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
# RUN useradd -m hduser && adduser hduser sudo && echo "hduser     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers && cd /usr/bin/ && sudo ln -s python3 python

WORKDIR /home/hduser
USER hduser
# 
# RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 0600 ~/.ssh/authorized_keys
ENV HADOOP_VERSION=3.3.3
ENV HADOOP_HOME=/home/hduser/hadoop-${HADOOP_VERSION}

ENV SPARK_VERSION=3.5.4
ENV SPARK_HOME=/home/hduser/spark-${SPARK_VERSION}

# 모든 셸에서 환경변수를 사용할 수 있도록 설정
RUN echo "export HADOOP_VERSION=${HADOOP_VERSION}" | sudo tee -a /etc/profile.d/hadoop.sh && \
    echo "export SPARK_VERSION=${SPARK_VERSION}" | sudo tee -a /etc/profile.d/hadoop.sh && \
    echo "export HADOOP_HOME=${HADOOP_HOME}" | sudo tee -a /etc/profile.d/hadoop.sh && \
    echo "export SPARK_HOME=${SPARK_HOME}" | sudo tee -a /etc/profile.d/hadoop.sh && \
    echo "export PATH=\${PATH}:\${HADOOP_HOME}/bin:\${SPARK_HOME}/bin" | sudo tee -a /etc/profile.d/hadoop.sh && \
    sudo chmod +x /etc/profile.d/hadoop.sh

# hduser의 .bashrc에도 추가
# RUN echo "source /etc/profile.d/hadoop.sh" >> /home/hduser/.bashrc && \
#     chown hduser:hduser /home/hduser/.bashrc

# pip, PySpark 설치 TODO: 나중에 합치자.
RUN sudo apt-get install -y software-properties-common \
    && sudo add-apt-repository -y ppa:deadsnakes/ppa \
    && sudo apt-get update -y \
    && sudo apt-get install -y python3.10 python3-pip \
    && sudo pip3 install --no-cache-dir pyspark==${SPARK_VERSION}

RUN cd /usr/bin/ \
    && sudo rm -rf python \
    && sudo ln -s python3.10 python \
    && sudo apt-get clean \
    && sudo rm -rf /var/lib/apt/lists/* \
    
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH

ENV HDFS_NAMENODE_USER=hduser
ENV HDFS_DATANODE_USER=hduser
ENV HDFS_SECONDARYNAMENODE_USER=hduser

ENV YARN_RESOURCEMANAGER_USER=hduser
ENV YARN_NODEMANAGER_USER=hduser

# 할 필요 없음
# COPY . /app

COPY missions/W4/M1/ssh_config /etc/ssh/ssh_config
COPY missions/W4/M1/entrypoints/ /home/hduser/
COPY missions/W4/M1/hadoop-configs/* $HADOOP_HOME/etc/hadoop/

# job 예시 파일 복사
COPY missions/W4/M1/src/main/python/ /home/hduser/src/main/python/

ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$SPARK_HOME/bin
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native

EXPOSE 8080 7077 6066

WORKDIR /usr/local/bin
WORKDIR /home/hduser

# # YARNSTART=0 will prevent yarn scheduler from being launched
ENV YARNSTART=0

