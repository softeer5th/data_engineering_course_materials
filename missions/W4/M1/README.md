# W4M1 - Building Apache Spark Standalone Cluster on Docker

## 개요
이 미션에서는 Apache Spark Standalone 클러스터를 설정하고, Spark를 사용하여 몬테카를로 방법으로 Pi 값을 계산한 뒤 HDFS에 결과를 저장한다. 목표는 Spark 작업을 원활히 실행하고 결과를 검증하는 것이다. 

---

## 환경 설정

### 1. Docker 클러스터 구성
- W3M2에서 구현했던 멀티 노드 클러스터 코드를 수정하며 사용한다.
- **서비스: Spark Master, Spark Worker 노드(2개), Hadoop Namenode, Datanode**
- **주요 특징:**
  - Spark 클러스터는 1개의 마스터와 2개의 워커 노드로 구성.
  - 분산 스토리지를 위한 HDFS Namenode와 Datanode 포함.
  - Spark와 Hadoop의 커스텀 설정으로 원활한 통합 보장.

### 2. 주요 포트
- **Spark Master UI:** `http://localhost:8080`
- **HDFS Namenode UI:** `http://localhost:9870`

---

## 파일 및 스크립트

### 1. `pi_with_output.py`
몬테카를로 방법으로 Pi 값을 계산하고 HDFS에 결과를 저장하는 Python 스크립트.
스파크 example로 내장된 pi.py를 수정, 결과를 csv로 저장하는 코드를 추가한다.

#### 주요 기능
- **SparkSession 생성:** `PythonPiWithOutput`라는 앱 이름으로 Spark 세션 초기화.
- **몬테카를로 계산:**
  - 무작위 점을 생성하고, 단위 원 내부에 위치한 점을 계산.
  - 원 내부 점의 비율로 Pi 값 계산.
- **HDFS 통합:**
  - 계산된 Pi 값을 지정된 경로에 단일 CSV 파일로 저장.
- **출력 검증:**
  - 결과가 HDFS에 단일 파일로 저장되었는지 확인.

```python
from __future__ import print_function
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    # SparkSession 생성
    spark = SparkSession.builder.appName("PythonPiWithOutput").getOrCreate()

    # 파티션 수 설정 (기본값 2)
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    # Monte Carlo 작업 정의
    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    # RDD를 사용해 Pi 값 계산
    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    pi = 4.0 * count / n
    print("Pi is roughly %f" % pi)

    # 결과를 DataFrame으로 변환
    result_df = spark.createDataFrame([(pi,)], ["pi_value"])

    # 결과 확인
    print("Checking result_df contents:")
    print(f"Number of rows in result_df: {result_df.count()}")
    result_df.show(truncate=False)

    # 결과를 CSV 파일로 저장
    HDFS_OUTPUT_PATH="hdfs://hadoop-namenode:9000/user/spark/output"
    result_df.coalesce(1).write.csv(HDFS_OUTPUT_PATH, mode="overwrite")
    print(f"Results successfully saved to {HDFS_OUTPUT_PATH}")

    # Spark 종료
    spark.stop()
```

### 2. `submit-pi-and-check.sh`
Spark 작업 제출 및 결과 검증을 자동화하는 Bash 스크립트.

#### 작업 단계
1. `/opt/spark-data`에 쓰기 권한 부여.
2. `spark-submit`으로 `pi_with_output.py` 작업 제출.
3. HDFS에서 출력 파일(`part-00000`)의 존재 여부 확인, 출력
```bash
# submit-pi-and-check.sh (컨테이너 내부용)
#!/bin/bash

# 저장 경로에 쓰기 권한 부여
echo "Setting write permissions for /opt/spark-data..."
chmod -R 777 /opt/spark-data

# Spark 작업 제출
echo "Submitting Spark job..."
if spark-submit --master spark://spark-master:7077 /opt/spark-data/pi_with_output.py; then
    echo "Spark job completed successfully."
else
    echo "Spark job failed."
    exit 1
fi

# 작업 결과 확인
RESULT_DIR="/user/spark/output"
echo "Checking output..."
if hdfs dfs -test -e "$RESULT_DIR/part-00000*"; then
    echo "Result file exists. Showing content:"
    hdfs dfs -cat "$RESULT_DIR/part-00000*"
else
    echo "Result file not found in $RESULT_DIR."
    exit 1
fi
```

---

## 수행 단계

### 1. HDFS 디렉토리 초기화
Spark 작업 실행 전:
- HDFS에서 필요한 디렉토리 생성:
  ```bash
  hdfs dfs -mkdir -p /user/spark/output
  hdfs dfs -chmod -R 777 /user/spark/output
  ```

### 2. Spark 작업 실행
- 다음 명령어로 작업 실행:
  ```bash
  /usr/local/bin/submit-pi-and-check.sh
  ```
- 스크립트에서 수행:
  - 작업 제출.
  - HDFS에서 출력 검증.

### 3. HDFS 출력 검증
- Pi 값을 포함한 출력 CSV 파일이 HDFS에 저장되었는지 확인:
  ```bash
  hdfs dfs -cat /user/spark/output/part-00000*
  ```

---

## 주요 결과
- Pi 값이 성공적으로 계산되고 HDFS에 저장됨:
  - 예시 출력: `3.13736`
- Spark와 HDFS 통합 확인 완료.
- `submit-pi-and-check.sh`를 사용한 자동 출력 검증 성공.

---

## 배운 점
- **Spark 작업 디버깅:** Spark 스크립트에서 로깅과 검증의 중요성.
- **자동화:** 작업 실행 및 결과 검증 자동화로 효율성 향상.
---

## 명령어 참조

### HDFS 출력 디렉토리 확인
```bash
hdfs dfs -ls /user/spark/output
```

### 기존 출력 삭제
```bash
hdfs dfs -rm -r /user/spark/output
```

### 출력 파일 내용 확인
```bash
hdfs dfs -cat /user/spark/output/part-00000*
```

