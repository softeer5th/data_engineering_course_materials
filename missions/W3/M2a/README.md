# W3M2a - Hadoop Multi-Node Cluster on Docker

## 개요

이 문서는 Hadoop 3.4.1을 실행할 수 있는 Docker 컨테이너 클러스터를 생성, 실행, 그리고 HDFS 작업을 수행하는 방법에 대한 단계별 지침을 제공합니다. 또한 이 문서는 멀티 노드 클러스터 설정하는 방법을 제공합니다.

---

## 1. Docker 이미지 빌드

### 1. 준비 작업

   `Dockerfile`, `hadoop-3.4.1.tar.gz`, `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`, `docker-compose.yml` 초기화 스크립트 `init.sh`, 그리고 `workers.txt` 파일을 동일한 디렉토리에 위치시킵니다.

### 2. 이미지 빌드
   아래 명령어를 사용하여 Docker 이미지를 빌드합니다:

   ```bash
   docker build -t hadoop-cluster .
   ```

`hadoop-cluster`는 생성된 이미지의 이름입니다.

### 3. 확인
   Docker 이미지가 성공적으로 빌드되었는지 확인하려면:

   ```bash
   docker images
   ```

목록에서 `hadoop-cluster`를 확인할 수 있습니다.

---

## 2. Docker Compose를 이용한 클러스터 실행

### 1. docker-compose.yml 파일 준비
   다음 내용을 포함하는 `docker-compose.yml` 파일을 생성합니다:

   ```yaml
   version: '3.9'

   services:
     master:
       build:
         context: .  # 현재 디렉토리에서 Dockerfile을 가져옴
       container_name: hadoop-master  # Master 컨테이너 이름
       environment:
         - NODE_TYPE=master  # Master 역할 지정
       ports:
         - "9870:9870"  # NameNode UI 포트
         - "9864:9864"  # DataNode UI 포트
         - "9000:9000"  # HDFS 포트
         - "8088:8088"  # ResourceManager 포트
         - "22:22"      # SSH 포트
       volumes:
         - ./volume/dfs/name:/opt/hadoop_data/hdfs/namenode  # NameNode 데이터 디렉토리
         - ./volume/dfs/data:/opt/hadoop_data/hdfs/datanode  # DataNode 데이터 디렉토리
       restart: always  # 컨테이너 중지 시 자동 재시작

     worker1:
       build:
         context: .  # 현재 디렉토리에서 Dockerfile을 가져옴
       container_name: hadoop-worker1  # Worker1 컨테이너 이름
       environment:
         - NODE_TYPE=worker  # Worker 역할 지정
       ports:
         - "9865:9864"  # DataNode UI 포트 (호스트와 겹치지 않도록 다른 포트 매핑)
         - "23:22"      # SSH 포트 (호스트와 겹치지 않도록 다른 포트 매핑)
       volumes:
         - ./volume/worker1/data:/opt/hadoop_data/hdfs/datanode  # DataNode 데이터 디렉토리
       restart: always  # 컨테이너 중지 시 자동 재시작

     worker2:
       build:
         context: .  # 현재 디렉토리에서 Dockerfile을 가져옴
       container_name: hadoop-worker2  # Worker2 컨테이너 이름
       environment:
         - NODE_TYPE=worker  # Worker 역할 지정
       ports:
         - "9866:9864"  # DataNode UI 포트 (호스트와 겹치지 않도록 다른 포트 매핑)
         - "24:22"      # SSH 포트 (호스트와 겹치지 않도록 다른 포트 매핑)
       volumes:
         - ./volume/worker2/data:/opt/hadoop_data/hdfs/datanode  # DataNode 데이터 디렉토리
       restart: always  # 컨테이너 중지 시 자동 재시작
   ```

### 2. 클러스터 실행
아래 명령어를 실행하여 클러스터를 시작합니다:

   ```bash
   docker-compose up -d
   ```

`-d` 옵션은 백그라운드에서 컨테이너를 실행합니다.

※ 이미지를 빌드하면서 컨테이너 실행하고 싶다면 다음과 같이 실행합니다:

    docker-compose up -d --build

### 3. 클러스터 확인
클러스터가 정상적으로 실행 중인지 확인하려면:

   ```bash
   docker ps
   ```

### 4. 하둡 실행 확인
NameNode UI에 접속하여 Hadoop이 정상적으로 실행 중인지 확인합니다:

   - 브라우저에서 `http://localhost:9870`으로 접속.

ResourceManager UI에 접속하여 각 노드에서 사용 가능한 리소스와 애플리케이션 상태를 확인할 수 있습니다.
- 브라우저에서 `http://localhost:8088`으로 접속
---

## 3. HDFS 작업 수행

클러스터가 시작되면 `init.sh` 스크립트가 자동으로 실행되어 HDFS와 YARN 서비스를 초기화합니다. 별도의 추가 작업 없이 바로 클러스터를 사용할 수 있습니다.

### 1. 컨테이너에 접속
실행 중인 컨테이너에 접속하려면 다음 명령어를 사용합니다:

    docker exec -it <컨테이너 이름> /bin/bash

HDFS에서 파일을 업로드하거나 다운로드하는 방법은 다음과 같습니다:

### 2. HDFS 파일 업로드
   파일을 HDFS에 업로드하려면:

   ```bash
   hdfs dfs -put <로컬_파일_경로> /<HDFS_경로>
   ```
   예:

   ```bash
   hdfs dfs -put example.txt /user/hdfs
   ```

### 3. HDFS 파일 확인
   업로드된 파일 목록을 확인하려면:

   ```bash
   hdfs dfs -ls /user/hdfs
   ```

### 4. HDFS 파일 다운로드
   파일을 HDFS에서 로컬로 다운로드하려면:

   ```bash
   hdfs dfs -get /<HDFS_파일_경로> <로컬_저장_경로>
   ```
   예:

   ```bash
   hdfs dfs -get /user/hdfs/example.txt ./
   ```

### 5. MapReduce 정상 실행 확인.

```bash
yarn jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 16 1000
```
YARN 클러스터에서 Hadoop의 MapReduce 프로그램을 실행하여 원주율을 계산하는 예제 파일을 진행합니다. 성공하면 정상적으로 실행됨을 뜻합니다.

---

## 4. 사용 가능한 포트

| 포트   | 설명                  |
| ---- | ------------------- |
| 22   | SSH 서비스             |
| 9870 | NameNode 웹 UI       |
| 9000 | NameNode 서비스        |
| 9864 | DataNode 서비스        |
| 8088 | ResourceManager UI   |

---

## 5. 참고 사항

### 1. 설정 파일

   - `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml` 파일은 Hadoop 클러스터의 올바른 실행을 위해 필수입니다.

### 2. 권한 문제

   - 각 노드의 HDFS 데이터 디렉토리(`/opt/hadoop_data`)는 컨테이너 내에서 관리됩니다. 권한 설정은 기본적으로 `root` 사용자로 처리됩니다.

### 3. 데이터 노드 추가

추가적인 데이터 노드를 구성하기 위해서는 다음과 같은 과정이 필요합니다. :

1. workers.txt에 추가할 datanode의 이름을 추가합니다.
2. docker-compose.yml 파일에 추가되는 worker에 대한 컨테이너 실행 요건을 추가해야합니다.

---

## 6. 참고 자료

- Hadoop 공식 문서: [Hadoop Documentation](https://hadoop.apache.org/docs/)
- Docker 공식 문서: [Docker Documentation](https://docs.docker.com/)

