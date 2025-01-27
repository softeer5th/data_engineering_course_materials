# Hadoop 싱글 노드 클러스터 설정 및 사용 방법

이 문서는 Docker를 사용하여 싱글 노드 Hadoop 클러스터를 설정하고 HDFS 작업을 수행하는 과정을 설명합니다.

---

## 1. Docker 이미지 생성

### Dockerfile 작성
`Dockerfile`을 다음과 같이 작성

```dockerfile
# 베이스 이미지 선택 (우분투)
FROM ubuntu:20.04

# 필요한 패키지 업데이트 및 설치
RUN apt-get update && apt-get install -y \
    wget ssh openjdk-8-jdk sudo && apt-get clean

# Hadoop 설치
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz && \
    tar -xzvf hadoop-3.4.1.tar.gz && \
    mv hadoop-3.4.1 /usr/local/hadoop && \
    rm hadoop-3.4.1.tar.gz

# 환경 변수 설정
ENV HADOOP_VERSION=3.4.1 \
    HADOOP_HOME=/usr/local/hadoop \
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64 \
    PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin

ENV HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root

# SSH 설정
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Hadoop 환경 설정 파일 복사
COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
COPY yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml

# Hadoop 데이터 디렉토리 생성 및 초기화
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data && \
    chown -R root:root /hadoop/dfs && \
    $HADOOP_HOME/bin/hdfs namenode -format

# 작업 디렉토리 설정
WORKDIR $HADOOP_HOME

# 컨테이너 시작 시 실행할 스크립트 복사
COPY start-hadoop.sh /usr/local/bin/start-hadoop.sh
RUN chmod +x /usr/local/bin/start-hadoop.sh

# Hadoop 포트 공개
EXPOSE 9870 9864 8088 8042 22

# 환경 변수 추가
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# 컨테이너 시작 시 스크립트 실행
CMD ["start-hadoop.sh"]
```

### `start-hadoop.sh` 스크립트 작성
```bash
#!/bin/bash

# SSH 서비스 시작
service ssh start

# Hadoop 서비스 시작
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# 컨테이너가 계속 실행되도록 유지
tail -f /dev/null
```

---

## 2. Docker 이미지 빌드
Dockerfile과 설정 파일이 있는 디렉토리에서 다음 명령어를 실행

```bash
docker build -t hadoop-single-node .
```

---

## 3. Docker 컨테이너 실행
HDFS 데이터가 컨테이너 재시작 시에도 유지되도록 볼륨을 설정하여 실행

우선 볼륨을 미리 생성
```bash
docker volume create hadoop-data
```
볼륨을 설정한 실행코드
```bash
docker run -it -p 9870:9870 -p 9864:9864 -p 8088:8088 -p 8042:8042 -p 22:22 \
    -v hadoop-data:/hadoop/dfs \
    --name hadoop-container hadoop-single-node
```

---

## 4. HDFS 디렉토리 및 파일 작업

### HDFS 디렉토리 생성
```bash
$HADOOP_HOME/bin/hdfs dfs -mkdir /user
```

### HDFS로 파일 업로드
Dockerfile과 동일한 디렉토리에 `sample.txt` 파일이 있다고 가정한다. 파일을 업로드하려면 다음 명령어를 실행한다

```bash
$HADOOP_HOME/bin/hdfs dfs -put /usr/local/hadoop/sample.txt /user
```

### HDFS에서 파일 확인
```bash
$HADOOP_HOME/bin/hdfs dfs -ls /user
```

### HDFS에서 파일 다운로드
HDFS에서 로컬로 파일을 다운로드하려면 다음 명령어를 사용한다

```bash
$HADOOP_HOME/bin/hdfs dfs -get /user/sample.txt /usr/local/hadoop/sample_downloaded.txt
```
웹 인터페이스로 업로드/ 다운로드하는 방법도 있다.

---

## 5. HDFS 웹 인터페이스 확인
HDFS 웹 UI는 브라우저에서 다음 주소로 접속하여 확인할 수 있다:

```
http://local:9870
```

---

## 6. 데이터 보존 확인
컨테이너를 중지했다가 다시 실행한 후에도 데이터가 유지되는지 확인한다:

```bash
docker stop hadoop-container
docker start hadoop-container
$HADOOP_HOME/bin/hdfs dfs -ls /user
```

---

## 7. 문제 해결

### 1. 네이티브 하둡 라이브러리 경고
경고가 출력되더라도 하둡은 정상적으로 동작한다.

```bash
WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
```

### 2. 포트 접근 문제
AWS EC2를 사용하는 경우, 보안 그룹 설정에서 필요한 포트를 열어야 한다.

이 과제에서는 EC2에 push하진 않음.

---

## 제출

- Dockerfile
- core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml
- start-hadoop.sh
- README.md

