# Amazon Product Review using MapReduce 프로젝트

- 이 문서는 Python Mapper와 Reducer 스크립트를 사용하여 제품군에서 제품 아이디, 리뷰 수, 평균 평점을 분석하는 Hadoop Streaming 작업을 설정하고 실행하는 방법에 대한 전체 가이드를 제공한다.
- Amazon Product Review using MapReduce는 W2M2에서 구성한 마스터1, 워커2의 멀티 노드 클러스터 환경에서 실행한다.
- Amazon Product Review using MapReduce를 위해 사용된 데이터는 다음과 같다. 아마존 리뷰 데이터셋 - All_Beauty 카테고리 [All_Beauty.jsonl](https://amazon-reviews-2023.github.io/)

---

## **개요**

이 프로젝트는 Hadoop Streaming을 사용하여 아마존 리뷰 데이터셋 - All_Beauty 카테고리의 제품 아이디, 리뷰 수, 평균 평점 분석을 수행하는 방법을 보여준다. 

작업에는 다음이 포함:
- jsonl의 각 행을 읽고 productId와 rating을 찾는 Python 기반 Mapper.
- 각 productID 별 리뷰 수와 평균 평점을 집계하는 Python 기반 Reducer.
- Streaming을 사용하여 Hadoop 클러스터에서 실행.

---
## **데이터셋**
- **이름**: Amazon Product Reviews
- **파일**: `All_Beauty.jsonl`
- **파일 형식**: JSON Lines(JSONL)
- **컬럼**:
  1. `asin`: 사용자 ID
  2. `rating`: 평점

---

## **필수 조건**

1. **Hadoop 설정**:
   - Hadoop이 설치되고 구성됨(싱글 노드 또는 멀티 노드 클러스터).
   - 환경 변수(`HADOOP_HOME`, `JAVA_HOME`)이 올바르게 설정됨.

2. **Python**:
   - 모든 Hadoop 노드에 Python 3이 설치되어 있어야 함.

3. **입력 파일**:
   - jsonl 파일(`All_Beauty.jsonl`)이 HDFS에 업로드되어 있어야 함.

4. **Python 스크립트**:
   - `mapper.py`와 `reducer.py` 스크립트.

---

## **실행 단계**

### **1. 멀티 노드 클러스터 환경 세팅**
- W3M2a에서 빌드한 멀티 노드 클러스터 환경을 그대로 사용
- 마스터 노드 하나, 워커 노드 둘을 docker-compose로 실행


jsonl파일 업로드를 위해 Dockerfile에 코드 추가
```
COPY All_Beauty.jsonl /app/All_Beauty.jsonl
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
hdfs dfs -put /app/All_Beauty.jsonl /user/test/
```

#### **2.3 업로드 확인**
```bash
hdfs dfs -ls /user/test
```

---

### **3. Python 스크립트**

#### **3.1 Mapper 스크립트 (`mapper.py`)**
Mapper는 각 행을 읽고 JSON 레코드를 처리하여 상품 ID(asin)과 평점(rating)의 키-값 쌍을 출력
```python
import sys
import json

# Mapper 코드
for line in sys.stdin: 
    line = line.strip() 
    try:
        # JSON 객체 파싱 -> python 딕셔너리로 변환
        review = json.loads(line)

        # 필요한 필드(상품ID, 평점) 추출
        product_id = review.get('asin', None)
        rating = review.get('rating', None)

        if product_id and rating:
            # 상품 ID와 평점 출력
            print(f"{product_id}\t{rating}")
    except json.JSONDecodeError:
        # JSON 파싱 실패 시 건너뜀
        continue

```

#### **3.2 Reducer 스크립트 (`reducer.py`)**
Reducer는 각 상품의 평점을 집계하고, 리뷰 개수와 평균 평점을 계산
```python
import sys

current_product = None  # 현재 상품 ID
current_sum = 0         # 평점 합계
current_count = 0       # 리뷰 개수

# Reducer 코드
for line in sys.stdin:
    line = line.strip()
    try:
        product_id, rating = line.split('\t')
        rating = float(rating)
    except ValueError:
        # 잘못된 데이터 건너뜀
        continue

    # 동일한 product_id에 대해 평점 누적
    if current_product == product_id:
        current_sum += rating
        current_count += 1
    else:
        # 이전 상품 출력
        if current_product:
            avg_rating = current_sum / current_count
            print(f"{current_product}\t{current_count}\t{avg_rating}")
        
        # 새로운 상품으로 초기화
        current_product = product_id
        current_sum = rating
        current_count = 1

# 마지막 상품 처리
if current_product:
    avg_rating = current_sum / current_count
    print(f"{current_product}\t{current_count}\t{avg_rating:.2f}")

```

---

### **4. Hadoop Streaming 작업 실행**

#### **4.1 MapReduce WordCount 실행**
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-input /user/test/All_Beauty.jsonl \
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

#### **4.5 리뷰 수 기준 상위 10개 출력**
```bash
hdfs dfs -cat /user/test/outputs/part-00000 | sort -k2 -nr | head -n 10
```
테스트한 결과는 아래와 같다. 
```
B007IAE5WY      1962    4.620285423037717
B00EEN2HCS      1750    4.135428571428571
B07C533XCW      1513    4.468605419695968
B00R1TAN7I      1372    4.033527696793003
B08L5KN7X4      1343    4.015636634400596
B019GBG0IE      1328    3.5338855421686746
B0719KWG8H      1168    4.478595890410959
B0092MCQZ4      1128    3.9849290780141846
B0107QYW14      1112    3.928956834532374
B0070Z7KME      934     4.279443254817987
```

---

### **5. 정리 작업**

#### **HDFS 출력 디렉토리 삭제**
테스트가 완료되면 출력 디렉토리를 삭제하여 공간을 확보
```bash
hdfs dfs -rm -r /user/test/outputs
```

