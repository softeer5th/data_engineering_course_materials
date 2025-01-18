# 베이스 이미지 선택 (리눅스 OS + Python)
FROM python:3.9

# 필요한 패키지 업데이트 & 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# 파이썬 라이브러리 설치
RUN pip install --upgrade pip
RUN pip install jupyterlab notebook
RUN pip install pandas
RUN pip install matplotlib

# Hadoop 설치
# 1. Hadoop 다운로드
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz \
    && tar -xzf hadoop-3.3.6.tar.gz \
    && mv hadoop-3.3.6 /usr/local/hadoop \
    && rm hadoop-3.3.6.tar.gz

# 2. 환경 변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# 3. Hadoop 기본 설정 (로컬 모드)
RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# 작업 디렉토리 설정
WORKDIR /app

# 로컬 디렉토리 내 ipynb 파일을 /app 디렉토리에 복사
COPY W1M1_mtcars_dataset_analysis.ipynb /app/
COPY data /app/data

# 컨테이너가 시작될 때, JupyterLab을 실행하도록 명령어 설정
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]