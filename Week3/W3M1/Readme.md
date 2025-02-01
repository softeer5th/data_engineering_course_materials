# **Hadoop Single-Node Docker 환경 설정 및 HDFS 작업**

이 프로젝트는 Docker를 사용하여 Hadoop 단일 노드 클러스터를 설정하고 HDFS 작업을 수행하는 방법을 설명합니다. 아래 단계에 따라 Docker 이미지를 빌드하고 컨테이너를 실행하며, HDFS에 데이터를 업로드하고 검색하는 작업을 진행할 수 있습니다.

---

## **사전 작업**

먼저 Hadoop에서 3.3.6 version의 binary (checksum signature) 파일을 다운로드 합니다.  
https://hadoop.apache.org/releases.html

---

## **1. Docker 이미지 빌드**

먼저 Dockerfile을 사용해 Hadoop 단일 노드 클러스터 환경을 생성합니다.

### **명령어**
```bash
docker build -t hadoop-single-node .
```

- `hadoop-single-node`라는 이름으로 Docker 이미지를 생성합니다.
- Dockerfile이 있는 디렉토리에서 명령어를 실행하세요.

---

## **2. 컨테이너 실행**

Docker 이미지를 기반으로 컨테이너를 실행합니다.

### **명령어**
```bash
docker run -it -p 9870:9870 -p 8088:8088 -v {path}/data:/usr/local/hadoop/data hadoop-single-node
```

- `-p 9870:9870`: HDFS 웹 UI를 호스트의 9870 포트에서 노출.
- `-p 8088:8088`: YARN ResourceManager UI를 호스트의 8088 포트에서 노출.
- `-v {path}/data:/usr/local/hadoop/data`: HDFS 데이터를 호스트 디렉토리 `{path}/data`에 영구적으로 저장.
- `{path}`는 로컬 경로로 변경하세요.

---

## **3. HDFS 작업 수행**

컨테이너 내부에서 HDFS 명령어를 사용해 작업을 수행합니다.

### **3.1 폴더 생성**

HDFS 내에 폴더를 생성합니다.

#### **명령어**
```bash
hdfs dfs -mkdir /{폴더}
```

- `{폴더}`: 생성할 폴더 이름. 예: `/test`.

---

### **3.2 로컬 파일 생성 후 HDFS에 업로드**

1. 로컬에서 간단한 텍스트 파일을 생성합니다.
2. 생성된 파일을 HDFS로 업로드합니다.

#### **명령어**
```bash
# 로컬 파일 생성
echo "hello world" > test.txt

# HDFS로 파일 업로드
hdfs dfs -put test.txt /{폴더}/test.txt
```

- `test.txt`: 로컬에서 생성된 파일.
- `/test/test.txt`: HDFS에 업로드될 경로.

---

### **3.3 업로드된 파일 확인**

HDFS에 업로드된 파일을 검색하고 내용을 확인합니다.

#### **명령어**
```bash
hdfs dfs -cat /test/test.txt
```

- `hello world`라는 내용이 출력되면 파일 업로드가 성공적으로 완료된 것입니다.

---

## **4. 추가 참고 사항**

### **Docker 컨테이너 내에서 HDFS의 네임노드(namenode)를 포맷**
```bash
hdfs namenode -format
```

### **HDFS UI 확인**
- **HDFS 웹 UI**: [http://localhost:9870](http://localhost:9870)
- **YARN ResourceManager UI**: [http://localhost:8088](http://localhost:8088)

### **컨테이너 중지**
컨테이너를 종료하려면 아래 명령을 사용합니다.

```bash
exit
```

### **컨테이너 재시작**
이미 실행 중인 컨테이너를 재시작하려면 아래 명령을 사용합니다.

```bash
docker start -ai <컨테이너_ID>
```

컨테이너 ID는 `docker ps -a` 명령으로 확인할 수 있습니다.

---
