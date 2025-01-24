# Hadoop Twitter Sentiment Analysis 프로젝트

- 이 문서는 Python Mapper와 Reducer 스크립트를 사용하여 csv 파일에서 감정을 분석하는 Hadoop Streaming 작업을 설정하고 실행하는 방법에 대한 전체 가이드를 제공한다.
- MapReduce Twitter Sentiment Analysis은 W2M2에서 구성한 마스터1, 워커2의 멀티 노드 클러스터 환경에서 실행한다.
- Twitter Sentiment Analysis를 위해 사용된 데이터는 다음과 같다. [Sentiment140](https://www.kaggle.com/datasets/kazanova/sentiment140)

---

## **개요**

이 프로젝트는 Hadoop Streaming을 사용하여 csv파일을 처리하고 감정을 분석을 수행하는 방법을 보여준다. 

작업에는 다음이 포함:
- 미리 정의된 키워드를 기준으로 감정을 분류하는 Python 기반 Mapper.
- 각 감정의 개수를 집계하는 Python 기반 Reducer.
- Streaming을 사용하여 Hadoop 클러스터에서 실행.
- 각 감정은 0이면 부정, 2면 중립, 4면 긍정으로 분류된다. 

---

## **필수 조건**

1. **Hadoop 설정**:
   - Hadoop이 설치되고 구성됨(싱글 노드 또는 멀티 노드 클러스터).
   - 환경 변수(`HADOOP_HOME`, `JAVA_HOME`)이 올바르게 설정됨.

2. **Python**:
   - 모든 Hadoop 노드에 Python 3이 설치되어 있어야 함.

3. **입력 파일**:
   - csv 파일(`training.1600000.processed.noemoticon.csv`)이 HDFS에 업로드되어 있어야 함.

4. **Python 스크립트**:
   - `mapper.py`와 `reducer.py` 스크립트.

---

## **실행 단계**

### **1. 멀티 노드 클러스터 환경 세팅**
- W3M2a에서 빌드한 멀티 노드 클러스터 환경을 그대로 사용
- 마스터 노드 하나, 워커 노드 둘을 docker-compose로 실행


csv파일 업로드를 위해 Dockerfile에 코드 추가
```
COPY training.1600000.processed.noemoticon.csv /app/training.1600000.processed.noemoticon.csv
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
hdfs dfs -put training.1600000.processed.noemoticon.csv /user/test/
```

#### **2.3 업로드 확인**
```bash
hdfs dfs -ls /user/test
```

---

### **3. Python 스크립트**

#### **3.1 Mapper 스크립트 (`mapper.py`)**
Mapper는 데이터셋의 각 줄을 처리하여 감정을 분류하고 키-값 쌍을 출력
```python
import sys
import csv

for line in sys.stdin:
    line = line.strip()  # 공백 제거
    try:
        # CSV 형식으로 한 줄 데이터를 파싱
        row = list(csv.reader([line]))[0]  # 한 줄 CSV 파싱
        target = row[0]  # 첫 번째 컬럼: target
        sentiment = "neutral"  # 기본값: neutral

        # target 값에 따라 감정 분류
        if target == "0":
            sentiment = "negative"
        elif target == "4":
            sentiment = "positive"

        # 결과 출력
        print(f"{sentiment}\t1")
    except Exception:
        # CSV 파싱 실패 시 무시
        continue

```

#### **3.2 Reducer 스크립트 (`reducer.py`)**
Reducer는 각 감정 카테고리의 개수를 집계하여 총합을 출력
```python
import sys

current_word = None #현재 단어
current_count = 0 #현재 단어의 누적 개수
word = None #입력에서 가져온 단어

for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1) #탭을 기준으로 최대 한번만 분리
    try: 
        count = int(count)
    except ValueError:
        continue
    
    #현재 단어와 이전 단어가 같다면 개수 누적
    if current_word == word:
        current_count += count
    #새로운 단어가 등장하면 이전 단어와 개수를 출력
    else:
        if current_word:
            print(f"{current_word}\t{current_count}")  # 이전 단어의 결과 출력
        current_word = word
        current_count = count

# 마지막 단어 출력
if current_word == word:
    print(f"{current_word}\t{current_count}")
```

---

### **4. Hadoop Streaming 작업 실행**

#### **4.1 MapReduce WordCount 실행**
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar \
-input /user/test/training.1600000.processed.noemoticon.csv \
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

#### **4.5 개수 기준 오름차순 정렬 출력**
```bash
hdfs dfs -cat /user/test/outputs/part-00000 | sort -k2 -n
```
테스트한 결과는 아래와 같다. 
```
negative        800000
positive        800000
```

---

### **5. 정리 작업**

#### **HDFS 출력 디렉토리 삭제**
테스트가 완료되면 출력 디렉토리를 삭제하여 공간을 확보
```bash
hdfs dfs -rm -r /user/test/outputs
```

