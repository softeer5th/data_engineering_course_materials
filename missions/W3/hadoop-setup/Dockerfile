FROM hadoop-base:latest

# 변수 설정 (docker-compose.yml에서 변경 가능)
ARG HADOOP_VERSION=3.4.1
ARG HADOOP_HOME=/usr/local/hadoop
ARG HADOOP_DATA=/usr/local/hadoop_data

# 환경 변수 설정
ENV HADOOP_HOME=${HADOOP_HOME}
ENV HADOOP_DATA=${HADOOP_DATA}
ENV HADOOP_STREAMING_JAR=${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-${HADOOP_VERSION}.jar
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
ENV HADOOP_YARN_ROLE=resourcemanager
ENV HADOOP_HDFS_ROLE=namenode

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul

# SSH 설정 (비밀번호 없이 작동하도록 임시 설정)
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# 하둡 설정 파일 복사
COPY config/ ${HADOOP_HOME}/etc/hadoop/

# start-hadoop.sh 복사
COPY start-hadoop.sh /start-hadoop.sh
RUN chmod +x /start-hadoop.sh

# stop-hadoop.sh 복사
COPY stop-hadoop.sh /stop-hadoop.sh
RUN chmod +x /stop-hadoop.sh

# 포트 노출
# 9870: HDFS Web UI, 9864: DataNode Web UI,
# 8088: ResourceManager Web UI, 8042: NodeManager Web UI, 
# 19888: JobHistory Web UI
EXPOSE 9870 9864 8088 8042 19888

# 컨테이너 실행 시 start-hadoop.sh 실행
CMD ["/start-hadoop.sh"]
