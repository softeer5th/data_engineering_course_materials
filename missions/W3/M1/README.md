# W3M1 - Hadoop Single-Node Cluster on Docker

## 개요

이 문서는 Hadoop 3.4.1을 실행할 수 있는 Docker 컨테이너를 생성, 실행, 그리고 HDFS 작업을 수행하는 방법에 대한 단계별 지침을 제공합니다. 또한 이 문서는 싱글 노드 클러스터 설정하는 방법을 제공합니다.

## 1. Docker 이미지 빌드

### 1. 준비 작업

`Dockerfile`, `hadoop-3.4.1.tar.gz`, `core-site.xml`, `hdfs-site.xml`, `docker-compose.yml` 그리고 초기화 스크립트 `init.sh` 파일을 동일한 디렉토리에 위치시킵니다.

### 2. 이미지 빌드
아래 명령어를 사용하여 Docker 이미지를 빌드합니다:

    docker build -t hadoop-container .

hadoop-container는 생성된 이미지의 이름입니다.

### 3. 확인
Docker 이미지가 성공적으로 빌드되었는지 확인하려면:

    docker images

목록에서 hadoop-container를 확인할 수 있습니다.

## 2. Docker Compose를 이용한 컨테이너 실행

### 1. docker-compose.yml 파일 준비
다음 내용을 포함하는 docker-compose.yml 파일을 생성합니다:

    version: '3.9'

    services:
    hadoop:
        build:
        context: .  # 현재 디렉토리에서 Dockerfile을 가져옴
        container_name: hadoop-container  # 컨테이너 이름
        ports:
          - "9870:9870"  # NameNode UI 포트
          - "9864:9864"  # DataNode UI 포트
          - "9000:9000"  # HDFS 포트
        volumes:
          - ./volume/dfs/name:/opt/hadoop_data/hdfs/namenode  # 호스트와 컨테이너 볼륨 연결
          - ./volume/dfs/data:/opt/hadoop_data/hdfs/datanode  # 호스트와 컨테이너 볼륨 연결
        restart: always  # 컨테이너가 중지될 경우 자동 재시작

### 2. 컨테이너 실행
아래 명령어를 실행하여 컨테이너를 시작합니다:

    docker-compose up -d

-d 옵션은 백그라운드에서 컨테이너를 실행합니다.

※ 이미지를 빌드하면서 컨테이너 실행하고 싶다면 다음과 같이 실행합니다:

    docker-compose up -d --build

--build는 이미지를 빌드해줍니다.

### 3. 컨테이너 확인
컨테이너가 정상적으로 실행 중인지 확인하려면:

    docker ps

### 4. 하둡 실행 확인
NameNode UI에 접속하여 Hadoop이 정상적으로 실행 중인지 확인합니다:

브라우저에서 http://localhost:9870으로 접속.

## 3. HDFS 작업 수행

컨테이너가 시작되면 init.sh 스크립트가 자동으로 실행되어 HDFS를 포맷하고 서비스를 시작합니다. 별도의 추가 작업 없이 바로 HDFS를 사용할 수 있습니다.

### 1. 컨테이너에 접속
실행 중인 컨테이너에 접속하려면 다음 명령어를 사용합니다:

    docker exec -it hadoop-container /bin/bash

HDFS에서 파일을 업로드하거나 다운로드하는 방법은 다음과 같습니다:

### 2. HDFS 파일 업로드
파일을 HDFS에 업로드하려면:

    hdfs dfs -put <로컬_파일_경로> /<HDFS_경로>

예:

    hdfs dfs -put example.txt /user/hdfs

### 3. HDFS 파일 확인
업로드된 파일 목록을 확인하려면:

    hdfs dfs -ls /user/hdfs

### 4. HDFS 파일 다운로드
파일을 HDFS에서 로컬로 다운로드하려면:

    hdfs dfs -get /<HDFS_파일_경로> <로컬_저장_경로>

예:

    hdfs dfs -get /user/hdfs/example.txt ./

## 4. 사용 가능한 포트

| 포트  | 설명                |
|-------|---------------------|
| 9870  | NameNode 웹 UI     |
| 9000  | NameNode 서비스    |
| 9864  | DataNode 서비스    |

## 5. 참고 사항

### 1. 설정 파일

core-site.xml과 hdfs-site.xml 파일은 Hadoop의 올바른 실행을 위해 필수입니다.

### 2. 권한 문제

HDFS 데이터 디렉토리(/opt/hadoop_data)는 hdfs 사용자에 의해 관리되므로 권한 설정을 신경 써야 합니다.

## 6. 참고 자료

Hadoop 공식 문서: [Hadoop Documentation](https://hadoop.apache.org/docs/)

Docker 공식 문서: [Docker Documentation](https://docs.docker.com/)

