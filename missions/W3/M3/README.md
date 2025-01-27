# Hadoop Word Count 프로젝트

- 이 문서는 Python Mapper와 Reducer 스크립트를 사용하여 텍스트 파일에서 단어 발생 빈도를 세는 Hadoop Streaming 작업을 설정하고 실행하는 방법에 대한 전체 가이드를 제공한다.
- MapReduce WordCount는 W2M2에서 구성한 마스터1, 워커2의 멀티 노드 클러스터 환경에서 실행한다.
- WordCount 테스트를 위해 사용된 전자책은 다음과 같다. 
[모비딕](https://www.gutenberg.org/ebooks/2701) - plain text UTF-8, 1.2MB

---

## **개요**

이 프로젝트는 Hadoop Streaming을 사용하여 텍스트 파일을 처리하고 단어 빈도를 계산하는 방법을 보여준다. 작업에는 다음이 포함됩니다:
- 단어와 그 개수를 출력하는 Python 기반 Mapper.
- 단어 개수를 집계하는 Python 기반 Reducer.
- Streaming을 사용하여 Hadoop 클러스터에서 실행.

---

## **필수 조건**

1. **Hadoop 설정**:
   - Hadoop이 설치되고 구성됨(싱글 노드 또는 멀티 노드 클러스터).
   - 환경 변수(`HADOOP_HOME`, `JAVA_HOME`)이 올바르게 설정됨.

2. **Python**:
   - 모든 Hadoop 노드에 Python 3이 설치되어 있어야 함.

3. **입력 파일**:
   - 텍스트 파일(`moby_dick.txt`)이 HDFS에 업로드되어 있어야 함.

4. **Python 스크립트**:
   - `mapper.py`와 `reducer.py` 스크립트.

---

## **실행 단계**

### **1. 멀티 노드 클러스터 환경 세팅**
- W3M2a에서 빌드한 멀티 노드 클러스터 환경을 그대로 사용
- 마스터 노드 하나, 워커 노드 둘을 docker-compose로 실행


웹 상의 전자책 파일 다운로드를 위해 Dockerfile에 코드 추가
```
RUN wget https://www.gutenberg.org/cache/epub/2701/pg2701.txt -O moby_dick.txt
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
hdfs dfs -put moby_dick.txt /user/test/
```

#### **2.3 업로드 확인**
```bash
hdfs dfs -ls /user/test
```

---

### **3. Python 스크립트**

#### **3.1 Mapper 스크립트 (`mapper.py`)**
```python
import sys

for line in sys.stdin:
    try:
        line = line.strip()  # 공백 제거
        words = line.split()  # 단어 분리
        for word in words:
            print(f"{word}\t1")  # 각 단어를 '단어\t1' 형식으로 출력
    except:
        continue
```

#### **3.2 Reducer 스크립트 (`reducer.py`)**
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
-input /user/test/moby_dick.txt \
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

#### **4.5 상위 10개 단어 확인(내림차순)**
```bash
hdfs dfs -cat /user/test/outputs/part-00000 | sort -k2 -nr | head -n 10
```
moby_dick.txt로 테스트한 결과는 아래와 같다. 
```
the     13862
of      6642
and     5997
a       4549
to      4531
in      3908
that    2691
his     2428
I       1723
with    1695
```

---

### **5. 트러블슈팅**

1. **출력 디렉토리 존재**:
   - 오류: `Output directory already exists`.
   - 아웃풋 파일을 넣고자 하는 경로가 이미 존재함
   - 해결 방법 - 경로 삭제 후 다시 시도
     ```bash
     hdfs dfs -rm -r /user/test/outputs
     ```

2. **Python이 없음**:
   - 모든 노드에서 `python3`가 설치되고 접근 가능해야 함.

3. **Mapper 또는 Reducer 오류**:
   - `mapper.py`와 `reducer.py` 스크립트를 독립적으로 확인.
   - Mapper 테스트:
     ```bash
     echo "word1 word2 word3" | python3 /usr/local/hadoop/mapper.py
     ```
   - Reducer 테스트:
     ```bash
     echo -e "word1\t1\nword2\t1\nword1\t1" | python3 /usr/local/hadoop/reducer.py
     ```

---

### **6. 정리 작업**

#### **HDFS 출력 디렉토리 삭제**
테스트가 완료되면 출력 디렉토리를 삭제하여 공간을 확보
```bash
hdfs dfs -rm -r /user/test/outputs
```

---

### **7. 요약**
- **Hadoop Streaming**: 텍스트 파일을 처리하는 데 사용됨
- **Python Mapper와 Reducer**: 단어 개수를 계산하기 위해 구현 및 테스트 완료
- **HDFS**: 입력/출력 데이터를 효율적으로 관리

