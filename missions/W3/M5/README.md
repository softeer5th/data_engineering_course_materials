# W3M5 - Average Rating of Movies using MapReduce

## 개요

이 문서는 Hadoop 3.4.1 멀티 노드 클러스터에서 영화의 평균 평점을 MapReduce 과정을 통해 계산하는 방법에 대한 단계별 지침을 제공합니다.

---

## 1. 파일 올리기

### 1. ratings.csv 파일 받기
https://grouplens.org/datasets/movielens/20m/ 사이트에서 쉽게 ratings.csv 파일을 다운로드 받을 수 있습니다.

### 2. 로컬 파일을 Container에 올리기

`ratings.csv`, `mapper_m5.py`, `reducer_m5.py`를 모두 컨테이너에 넣어줍니다. :

    docker cp <로컬 파일 경로>  <컨테이너 이름>:<컨테이너 내 파일 경로>

### 3. 컨테이너에 접속
실행 중인 컨테이너에 접속하려면 다음 명령어를 사용합니다:

    docker exec -it <컨테이너 이름> /bin/bash

### 4. HDFS에 ratings.csv 올리기

ratings은 csv 데이터로서 하둡으로 올려야하기 때문에 하둡에 폴더를 만들어주고, 그 공간에 파일을 올려줍니다. :

    hdfs dfs -mkdir -p <하둡에 저장할 폴더 경로>
    hdfs dfs -put /tmp/ratings.csv.csv<하둡에 저장할 폴더 경로>

### 5. 업로드 확인
파일이 원하는 경로에 올라갔는지 확인합니다. :

    hdfs dfs -ls /input

ratings.csv가 출력된다면 정상적으로 실행된 것입니다.

## 2. MapReduce

### 1. MapReduce 실행
Hadoop Streaming JAR 파일을 실행합니다. 이 파일은 Hadoop에서 mapper와 reducer가 python으로 작성된 경우 이를 실행하는데 사용됩니다.

    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /input/ratings.csv \
    -output <파일 경로명> \
    -mapper "python3 mapper_m5.py" \
    -reducer "python3 reducer_m5.py" \
    -file "<mapper 파일 경로>" \
    -file "<reducer 파일 경로>"

### 2. 결과 확인

    hdfs dfs -cat <하둡 내 결과 파일 경로>/part-00000

### 3. 결과 파일 가져오기

    hdfs dfs -get <하둡 내 결과 파일 경로>/part-00000 <저장할 로컬 파일 경로>/<파일 이름>

## 3. 참고 사항

### 1. MapReduce 실행 실패 
MapReduce 실행 실패시 결과 파일 디렉토리 제거해줘야지만, MapReduce 실행 코드를 실행할 수 있다.

    hdfs dfs -rm -r <하둡 내 결과 파일 경로>