# Hadoop Average Rating of Movie 프로젝트

- 이 문서는 Python Mapper와 Reducer 스크립트를 사용하여 csv 파일에서 영화 별 평균 평점을 분석하는 Hadoop Streaming 작업을 설정하고 실행하는 방법에 대한 전체 가이드를 제공한다.
- Average Rating of Movie는 W2M2에서 구성한 마스터1, 워커2의 멀티 노드 클러스터 환경에서 실행한다.
- Average Rating of Movie를 위해 사용된 데이터는 다음과 같다. [MovieLens 20M Dataset](https://grouplens.org/datasets/movielens/20m/)

---

## **개요**

이 프로젝트는 Hadoop Streaming을 사용하여 csv파일을 처리하고 영화 별 평균 평점 분석을 수행하는 방법을 보여준다. 

작업에는 다음이 포함:
- csv의 각 행을 읽고 movieId와 rating을 찾는 Python 기반 Mapper.
- 각 movieID 별 평균 평점을 집계하는 Python 기반 Reducer.
- Streaming을 사용하여 Hadoop 클러스터에서 실행.

---
## **데이터셋**
- **이름**: MovieLens 20M Dataset
- **파일**: `ratings.csv`
- **컬럼**:
  1. `userId`: 사용자 ID
  2. `movieId`: 영화 ID
  3. `rating`: 평점
  4. `timestamp`: 타임스탬프

- **데이터 예시**:
  ```plaintext
  1,2,3.5,1112486027
  1,29,3.0,1112484676
  2,32,4.0,1112484819
  ```

---

## **필수 조건**

1. **Hadoop 설정**:
   - Hadoop이 설치되고 구성됨(싱글 노드 또는 멀티 노드 클러스터).
   - 환경 변수(`HADOOP_HOME`, `JAVA_HOME`)이 올바르게 설정됨.

2. **Python**:
   - 모든 Hadoop 노드에 Python 3이 설치되어 있어야 함.

3. **입력 파일**:
   - csv 파일(`ratings.csv`)이 HDFS에 업로드되어 있어야 함.

4. **Python 스크립트**:
   - `mapper.py`와 `reducer.py` 스크립트.

---

## **실행 단계**

### **1. 멀티 노드 클러스터 환경 세팅**
- W3M2a에서 빌드한 멀티 노드 클러스터 환경을 그대로 사용
- 마스터 노드 하나, 워커 노드 둘을 docker-compose로 실행


csv파일 업로드를 위해 Dockerfile에 코드 추가
```
COPY ratings.csv /app/ratings.csv
```
---

### **2. HDFS 환경 준비**

#### **2.1 HDFS 입력 디렉토리 생성**
```bash
hdfs dfs -mkdir -p /user
hdfs dfs -mkdir -p /user/test
```

#### **2.2  파일 업로드**
```bash
hdfs dfs -put /app/ratings.csv /user/test/
```

#### **2.3 업로드 확인**
```bash
hdfs dfs -ls /user/test
```

---

### **3. Python 스크립트**

#### **3.1 Mapper 스크립트 (`mapper.py`)**
Mapper는 각 행을 읽고 movieId와 rating을 키-값 쌍으로 출력
```python
import sys
import csv

is_header = True #헤더 확인 플래그

for line in sys.stdin:
    line = line.strip()  # 공백 제거
    try:
        # CSV 데이터 파싱
        row = list(csv.reader([line]))[0]

        # 첫 번째 줄(헤더) 건너뛰기
        if is_header:
            is_header = False
            continue

        movie_id = row[1]  # 두 번째 컬럼: movieId
        rating = row[2]    # 세 번째 컬럼: rating

        # movieId와 rating 출력
        print(f"{movie_id}\t{rating}")
    except Exception:
        continue
```

#### **3.2 Reducer 스크립트 (`reducer.py`)**
Reducer는 동일한 movieId에 대해 모든 rating을 집계한 후 평균을 계산합니다.
```python
import sys

current_movie = None #현재 영화의 아이디
current_sum = 0 #현재 영화의 누적 평점
current_count = 0

# Reducer code
for line in sys.stdin:
    line = line.strip()
    movie_id, rating = line.split('\t')
    rating = float(rating) #평점 형변환
    
    #현재 영화와 이전 영화가 같다면 개수 누적
    if current_movie == movie_id:
        current_sum += rating
        current_count += 1 
    #새로운 영화가 등장하면 이전 영화의 아이디와 평균 평점을 출력
    else:
        if current_movie:
            avg_rating = current_sum / current_count
            print(f"{current_movie}\t{avg_rating}") 
        #변수 초기화
        current_movie = movie_id
        current_sum = rating
        current_count = 1

# 마지막 영화 처리
if current_movie == movie_id:
    avg_rating = current_sum / current_count
    print(f"{current_movie}\t{avg_rating}") 
```

---

### **4. Hadoop Streaming 작업 실행**

#### **4.1 MapReduce WordCount 실행**
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-input /user/test/ratings.csv \
-output /user/test/outputs \
-mapper "python3 /usr/local/hadoop/mapper.py" \
-reducer "python3 /usr/local/hadoop/reducer.py"
```

#### **4.2 작업 완료 확인**
`_SUCCESS` 파일 확인:
  ```bash
  hdfs dfs -ls /user/test/outputs
  ```

#### **4.3 HDFS에서 출력 표시**
```bash
hdfs dfs -cat /user/test/outputs/part-00000
```

#### **4.4 출력 파일 로컬로 다운로드**
```bash
hdfs dfs -get /user/test/outputs ./local_outputs
cat ./local_outputs/part-00000
```

#### **4.5 평점 기준 하위 10개 출력**
```bash
hdfs dfs -cat /user/test/outputs/part-00000 | sort -k2 -n | head -n 10
```
테스트한 결과는 아래와 같다. 
```
100103  0.5
100203  0.5
101595  0.5
101634  0.5
101728  0.5
102176  0.5
102459  0.5
103294  0.5
103614  0.5
103695  0.5
```

---

### **5. 정리 작업**

#### **HDFS 출력 디렉토리 삭제**
테스트가 완료되면 출력 디렉토리를 삭제하여 공간을 확보
```bash
hdfs dfs -rm -r /user/test/outputs
```

