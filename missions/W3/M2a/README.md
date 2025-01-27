
# Hadoop 멀티 노드 클러스터 설정 및 사용 방법

이 문서는 Docker를 사용하여 멀티 노드 Hadoop 클러스터를 설정하고 HDFS 작업을 수행하는 과정을 설명합니다.

---

## 1. Docker 이미지 생성

### Dockerfile 작성
`Dockerfile`을 다음과 같이 작성한다.

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

# 환경 변수 설정(하둡 버전, 하둡 경로, 자바 경로, path설정)
ENV HADOOP_VERSION=3.4.1 \
    HADOOP_HOME=/usr/local/hadoop \
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64 \
    PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin \
    HADOOP_MAPRED_HOME=/usr/local/hadoop

# 환경 변수 설정
ENV HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root

# SSH 설정 (Hadoop은 SSH로 내부 통신을 함)
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Hadoop 환경 설정
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

# Hadoop 환경설정 파일에 JAVA_HOME 환경변수 추가
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh


# 컨테이너 시작 시 스크립트 실행
CMD ["start-hadoop.sh"]

```

### `start-hadoop.sh` 스크립트 작성
```bash
#!/bin/bash

# SSH 서비스 시작
service ssh start

# Master 노드에서만 NameNode 및 ResourceManager 실행
if [[ $HOSTNAME == "master" ]]; then
    # $HADOOP_HOME/bin/hdfs namenode -format -force || true
    $HADOOP_HOME/sbin/start-dfs.sh
    $HADOOP_HOME/sbin/start-yarn.sh
    tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log
fi

# Worker 노드는 DataNode 및 NodeManager 실행
if [[ $HOSTNAME != "master" ]]; then
    $HADOOP_HOME/bin/hdfs --daemon start datanode
    $HADOOP_HOME/bin/yarn --daemon start nodemanager
    tail -f $HADOOP_HOME/logs/hadoop-*-datanode-*.log
fi
```

---

## 2. Docker 이미지 빌드
Dockerfile과 설정 파일이 있는 디렉토리에서 다음 명령어를 실행한다.

```bash
docker build -t hadoop-multi-node .
```

---

## 3. Docker 컨테이너 실행 - Docker Compose
Docker Compose 파일을 이용해 마스터, 워커1, 워커2 노드가 한번에 같이 실행되도록 한다.
- 각 컨테이너는 하나의 동일한 도커 네트워크(hadoop-network)에 연결됨
- 각 컨테이너는 한 이미지(hadoop-multi-node)로 실행
- 마스터 노드는 포트를 열어놓음/ 워커노드는 마스터와만 통신하므로 포트 열지않음
- 각 컨테이너는 각각의 볼륨에 연결되어 데이터 영속성 보장


```bash
version: '3.8' #Docker Compose 파일의 버전

services:
  master:
    image: hadoop-multi-node
    container_name: hadoop-master
    hostname: master
    ports:
      - "9870:9870"   # HDFS Web UI
      - "8088:8088"   # YARN Web UI
      - "22:22"       # SSH
    volumes:
      - hadoop-master-name:/hadoop/dfs/name
    networks:
      - hadoop-network

  worker1:
    image: hadoop-multi-node
    container_name: hadoop-worker1
    hostname: worker1
    volumes:
      - hadoop-worker1-data:/hadoop/dfs/data
    networks:
      - hadoop-network

  worker2:
    image: hadoop-multi-node
    container_name: hadoop-worker2
    hostname: worker2
    volumes:
      - hadoop-worker2-data:/hadoop/dfs/data
    networks:
      - hadoop-network

volumes:
  hadoop-master-name:  # 마스터 노드의 메타데이터 저장
  hadoop-worker1-data: # 워커1 노드의 데이터 저장
  hadoop-worker2-data: # 워커2 노드의 데이터 저장

#네트워크는 compose up/down 실행시마다 생성 및 삭제됨
networks:
  hadoop-network:
    driver: bridge


```

### 컴포즈 파일 run
```bash
docker-compose up -d 
```

### 컨테이너 중지/ 다시 실행
docker-compose down 명령어는 볼륨 자체는 유지되지만 데이터 디렉토리가 초기화됨

docker-compose stop/ start를 이용해서 컨테이너만 중지, 네트워크와 볼륨은 그대로 유지한다. 
```bash
docker-compose stop
docker-compose start
```

---
## 4. HDFS 디렉토리 및 파일 작업

### HDFS 디렉토리 생성
```bash
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/test/
```
### 로컬에서 테스트 파일 생성
```bash
echo "This is a test file for Hadoop Hadoop cluster" > /tmp/testfile.txt
```
### 파일을 HDFS에 업로드
```bash
$HADOOP_HOME/bin/hdfs dfs -put /tmp/testfile.txt /user/test/
 ```
### HDFS에 업로드된 파일 확인
```bash
$HADOOP_HOME/bin/hdfs dfs -ls /user/test
```
### HDFS에서 로컬 파일 시스템으로 파일 다운로드
```bash
$HADOOP_HOME/bin/hdfs dfs -get /user/test/testfile.txt /tmp/testfile_downloaded.txt
```
### 다운로드된 파일 내용 확인
```bash
cat /tmp/testfile_downloaded.txt
```
---

## 5. MapReduce 테스트

### 위에서 만든 파일을 MapReduce를 이용해서 WordCount 해본다
### MapReduce 작업 실행
```bash
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /user/test /user/output
```
- user/test에 있는 파일을 읽어서 Map, Reduce, 그 결과를 user/output 디렉토리에 저장한다
- 출력되는 내용
    - 리소스매니저 연결됨
    - MapReduce 작업 제출
    - 작업 상태 업데이트 map -> reduce
    - 작업 완료

### 결과 저장된 파일 확인
카운트 된 단어와 단어 별 개수가 나옴
```bash
$HADOOP_HOME/bin/hdfs dfs -cat /user/output/part-r-00000
```
 
### 결과 파일 로컬로 다운받기
```bash
$HADOOP_HOME/bin/hdfs dfs -get /user/output/part-r-00000 ./output.txt
```

### 다운로드 된 파일 내용 확인
```bash
cat ./output.txt
```

---

## 6. 웹 인터페이스 확인
HDFS 웹 UI는 브라우저에서 다음 주소로 접속하여 확인할 수 있다

```
http://loacl:9870
```
YARN 웹 UI는 브라우저에서 다음 주소로 접속하여 확인할 수 있다
```
http://loacl:8088
```

---

## 7. 데이터 보존 확인
컨테이너를 중지했다가 다시 실행한 후에도 데이터가 유지되는지 확인한다

(down 말고 stop/ start 사용)

```bash
docker-compose stop
docker-compose start
$HADOOP_HOME/bin/hdfs dfs -ls /user
```

---

## 8. 문제 해결

### 1. 네이티브 하둡 라이브러리 경고
경고가 출력되더라도 하둡은 정상적으로 동작함

```bash
WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
```

### 2. 포트 접근 문제
AWS EC2를 사용하는 경우, 보안 그룹 설정에서 필요한 포트를 열어야 합니다.

---

## 제출

- Dockerfile
- core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml
- start-hadoop.sh
- docker-compose.yml
- README.md (이 문서)


