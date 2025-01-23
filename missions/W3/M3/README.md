# Hadoop Word Count 프로젝트

이 문서는 Python Mapper와 Reducer 스크립트를 사용하여 텍스트 파일에서 단어 발생 빈도를 세는 Hadoop Streaming 작업을 설정하고 실행하는 방법에 대한 전체 가이드를 제공한다.

MapReduce WordCount는 W2M2에서 구성한 마스터1, 워커2의 멀티 노드 클러스터 환경에서 실행한다.

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
   - 텍스트 파일(`testfile.txt`)이 HDFS에 업로드되어 있어야 함.

4. **Python 스크립트**:
   - `mapper.py`와 `reducer.py` 스크립트.

---

## **실행 단계**

### **1. HDFS 환경 준비**

#### **1.1 HDFS 입력 디렉토리 생성**
```bash
hdfs dfs -mkdir -p /user/test
```

#### **1.2 입력 파일 업로드**
```bash
hdfs dfs -put testfile.txt /user/test/
```

#### **1.3 업로드 확인**
```bash
hdfs dfs -ls /user/test
```

---

### **2. Python 스크립트**

#### **2.1 Mapper 스크립트 (`mapper.py`)**
```python
import sys

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    for word in words:
        print(f"{word}\t1")
```

#### **2.2 Reducer 스크립트 (`reducer.py`)**
```python
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    count = int(count)

    if current_word == word:
        current_count += count
    else:
        if current_word:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

if current_word == word:
    print(f"{current_word}\t{current_count}")
```

---

### **3. Hadoop Streaming 작업 실행**

#### **3.1 명령어 실행**
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar \
-input /user/test/testfile.txt \
-output /user/test/outputs \
-mapper "python3 /usr/local/hadoop/mapper.py" \
-reducer "python3 /usr/local/hadoop/reducer.py"
```

#### **3.2 작업 완료 확인**
- `_SUCCESS` 파일 확인:
  ```bash
  hdfs dfs -ls /user/test/outputs
  ```

---

### **4. 출력 확인**

#### **4.1 HDFS에서 출력 표시**
```bash
hdfs dfs -cat /user/test/outputs/part-00000
```

#### **4.2 출력 파일 로컬로 다운로드**
```bash
hdfs dfs -get /user/test/outputs ./local_outputs
cat ./local_outputs/part-00000
```

#### **4.3 단어 개수로 출력 정렬**
```bash
hdfs dfs -cat /user/test/outputs/part-00000 | sort -k2 -n
```

---

### **5. 문제 해결**

#### **일반적인 문제**:
1. **출력 디렉토리 존재**:
   - 오류: `Output directory already exists`.
   - 해결 방법:
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

### **6. 출력 예제**

#### **HDFS 출력**:
```plaintext
word1	2
word2	3
word3	1
```

#### **빈도별 정렬 출력**:
```plaintext
word3	1
word1	2
word2	3
```

---

### **7. 정리 작업**

#### **HDFS 출력 디렉토리 삭제**
```bash
hdfs dfs -rm -r /user/test/outputs
```

---

### **8. 요약**
- **Hadoop Streaming**: 텍스트 파일을 처리하는 데 성공적으로 사용됨.
- **Python Mapper와 Reducer**: 단어 개수를 계산하기 위해 구현 및 테스트 완료.
- **HDFS**: 입력/출력 데이터를 효율적으로 관리.

이 가이드는 Hadoop Streaming을 사용한 Word Count 작업의 설정, 실행 및 출력 분석에 대한 전체 프로세스를 다룹니다. 추가적인 도움이 필요하면 언제든지 문의하세요!

