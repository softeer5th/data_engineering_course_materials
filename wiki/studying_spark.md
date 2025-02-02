# 스파크

## 개요

회고에 적었던 Spark 관련 내용들을 옮겨 왔다. 여기에서 이어 적으려고 한다.

## 미션 1

> * Spark를 설치 및 실행하여 M1을 진행해보자. (진행하며 작성 중)

**0. 시작에 앞서 미션을 파악해보자**

```
1. 1개의 마스터 노드와 2개의 워커 노드로 구성된 standalone 클러스터를 실행해야 한다.
2. Spark Job은 input 경로에서 데이터를 읽고 output 경로에 결과를 써야 한다.
   * 이 과정에서 CSV, parquet 등으로 결과를 저장해야 한다.
```

**미션을 읽고 난 뒤 다음의 막연한 의문들이 떠올랐다.**

* Spark는 YARN과 같은 자원 관리 계층의 요소와의 통합을, 내가 작성한 Spark 코드의 수정 없이 추후 필요할 시 쉽게 해낼 수 있는가? 물론 그래야 할 것 같긴 하지만, 정말 그럴까..
* 지난 주 미션에서 작성했던 Hadoop Streaming 방식을 이용한 MapReduce 사용 코드의 출력 결과는 어떤 형태로 HDFS에 저장되었던 것일까? 또한 CSV나 parquet을 HDFS에 저장할 수 있는 건가? 가능 여부와 별개로 HDFS에 데이터를 작성할 때 따로 형식이 있었던가.. 물론 input 데이터를 HDFS에 put할 때는 파일 형식을 그대로 넣긴 했었다. (물론 현재 미션에서는 HDFS를 사용하는 것이 아니라 그냥 linux 파일시스템(호스트 OS든 컨테이너 안이든..)에 저장하라는 것 같긴 하다) 저장 형식에 대한 제한은 딱히 없는 것인가? (그렇다고 생각하는 게 타당한 것 같다.)
  * 내가 지난 주 미션을 Word Count까지만 진행했기에 학습 과정에서 뭔가 놓친 것이 있을 수도 있다. 빠르게 Spark 미션을 끝내고 마치지 못했던 지난 Hadoop 미션을 다시 해야겠다.

**빠른 파악을 위해 AI에게 질문을 하여 답변을 얻었다. 물론 미션을 수행하며 검증을 해야 한다.**

* 그렇다. spark-submit 시 YARN의 사용 여부를 지정할 수 있는 것 보면, spark를 사용하는, 가령 파이썬 파일의 내부 코드를 수정할 필요가 없다.
    ```bash
    # Standalone 모드
    spark-submit --master spark://master:7077 program.py
    
    # YARN 모드
    spark-submit --master yarn program.py
    ```
    * 다만 standalone, yarn, ... 과 같은 cluster 자체의 구조는 이미 구성되어 있어야 하는 것이 아닌가? 즉 YARN을 이용하는 cluster를 구성했다면 제출 시 yarn만을 선택해야 한다.
    * 월요일 강의에서 `deploy mode`가 있다는 것을 배웠는데, 사실상 이미 구성된 cluster 위에서 자유롭게 선택할 수 있는 것은 `deploy mode` 뿐인 것 같다. 
      * 그런데 실제로 실행을 해보니, standalone cluster에서는 deploy mode를 `cluster`로 설정할 수 없었다.
      * 애초에 이 옵션들을 굳이 동적으로 바꿔야 할 상황은 많지 않을 것 같고, 목적에 맞게 cluster 및 인프라를 구성했으면 그에 맞도록 옵션을 줘서 Spark를 실행하면 될 듯.
* MapReduce의 결과를 단순히 stdout에 출력했다면 텍스트 형식으로 저장된다. 다만 HDFS는 말그대로 '파일 시스템'이니까, 거기에 어떤 형식의 파일을 저장할지는 별개의 문제다. 이번 미션에서는 parquet 파일을 HDFS에 저장하면 되는 것.

첫 의문에 대해서 좀 더 생각해보면, 결국 pyspark와 같이 사용자에게 특정 언어의 라이브러리 형태로 제공되는 인터페이스를 사용하게 되면 내부적으로 알아서 적절한 분산 처리를 한다는 것인가?

일단 미션을 수행하며 남은 의문들에 대한 답을 찾아보자.

---

**1. 도커 파일을 interactive하게 작성해 보자.**

지난 주 도커 파일을 작성하면서 많은 시간을 낭비했으므로.. 이번에는 같은 실수를 반복하지 말아야 한다.

저번에 도커 파일을 작성할 때는 막연히 확장성을 고려하고자 base 이미지로 ubuntu를 선택했었는데, 시간 단축을 위해 Java 정도는 기본적으로 깔려 있는 이미지를 선택해 보자. 애초에 한 컨테이너가 여러 기능을 담당하는 것이 별로 좋은 방향은 아닌 것 같고, 특정 버전의 Java가 설치된 이미지를 선택한다고 해서 확장성이 떨어질 이유도 딱히 없다. 

`eclipse-temurin:11-jre` 이미지를 이용하기로 했다. 어차피 컨테이너에서는 이미 빌드 혹은 컴파일된 자바 코드 및 파이썬 스크립트만을 실행하면 되므로, jdk는필요 없고 jre만 있으면 된다.

또한 Hadoop을 컴파일하기 위해서는 Java 8을 이용하는 것을 권장한다는데.. 마찬가지로 Hadoop 자체를 컴파일할 일이 (현재는) 없으므로 위 이미지로 결정했다.

---

**Spark 설치 방법을 찾아보다가, pyspark 같은 것들을 따로 설치해야 하는지 궁금해졌다. 검색을 해보니 Spark 다운 시 내부에 python 패키지들이 포함되어 있다고 하는데..**

* 지난번처럼 무작정 도커 파일을 작성 및 수정하는 게 아니라 직접 컨테이너 내부에서 다운로드 후 확인을 해보면 된다.
* 확인 결과 pyspark 패키지가 존재했다. 다만 python 인터프리터는 포함되어 있지 않아 따로 설치해줘야 한다는 것을 알게 되었다.

```
root@f501aa96ef6d:/usr/local/spark/python# ls
dist  MANIFEST.in  pyspark.egg-info  run-tests.py             setup.py
docs  mypy.ini     README.md         run-tests-with-coverage  test_coverage
lib   pyspark      run-tests         setup.cfg                test_support
root@f501aa96ef6d:/usr/local/spark/python# ls lib/
py4j-0.10.9.7-src.zip  PY4J_LICENSE.txt  pyspark.zip
```

`pyspark`가 두 군데에 존재했는데.. 검색을 해보니 python은 zip파일도 import를 할 수 있다고 한다. 어차피 `py4j`도 import를 해야하니, zip 파일이 존재하는 경로를 `PYTHONPATH` 환경변수에 추가하기로 하였다.

---

이제 Spark를 실행해 보자.

```bash
spark-submit $SPARK_HOME/examples/src/main/python/pi.py
```

위 명령어를 실행하니 아래와 같은 결과가 출력됐다. 옵션을 주지 않았더니 `local`모드로 실행되었다.

```bash
Pi is roughly 3.131000
```

조금 더 명시적인 명령어는 다음과 같다.

```bash
# 로컬 머신의 모든 코어를 사용하여 Spark를 local mode로 실행
spark-submit --master local[*] $SPARK_HOME/examples/src/main/python/pi.py
```

이로써 Spark가 정상적으로 설치되었음을 확인하였고, 실행했던 명령어들을 도커 파일에 옮겨 작성할 수 있게 되었다.

---

**2. docker-compose.yml을 작성하여 standalone cluster를 실행해 보자.**

시작에 앞서 궁금한 점이 생겼다.

* 각 노드들은 어떻게 통신하는지? 보안은 어떻게 달성되는가?
* Hadoop의 xml 형식의 설정 파일 같은 것이 존재하는지?

AI에게 질문을 하여 정보를 얻을 수 있었다.

* Master와 Worker는 Akka 프레임워크 기반의 RPC를 사용한다.
* Driver와 Executor는 Netty 기반의 RPC를 사용한다.
* 기본적으로는 암호화되지 않은 통신을 사용한다. 필요시 옵션을 줄 수 있다.

* `SPARK_HOME/conf` 디렉터리에 설정 파일이 존재한다.
  * 역시나 수많은 옵션들이 있다..

마찬가지로 풀리지 않은 의문은 미션을 수행하며 알아 보기로 하자.

---

**먼저 최소한의 설정을 구성한 뒤 실행을 해 보자.**

`spark-env.sh`에 어떤 내용을 적어야 할지는 다음 문서에 나타나있다. 필요한 환경 변수를 작성하면 된다.

* https://spark.apache.org/docs/latest/configuration.html#environment-variables
* https://spark.apache.org/docs/latest/spark-standalone.html#cluster-launch-scripts

`spark-defaults.conf`에 적을 내용은 다음 문서를 확인하도록 하자.

* https://spark.apache.org/docs/latest/configuration.html#available-properties
* https://wikidocs.net/107317

위 문서들을 참고하여 적절히 설정 파일들을 작성했고, 컨테이너들을 실행할 수 있었다.

일단 볼륨 마운트는 하지 않았고, 직접 master container에 접속한 뒤 아래 명령어를 실행해 보았다.

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  $SPARK_HOME/examples/src/main/python/pi.py 10
```

다행히 오류는 발생하지 않았고 다음 결과를 얻을 수 있었다.

```
Pi is roughly 3.145960
```

---

**3. 이제 남은 세부 요구사항을 구현해야 한다.**

* The Spark job should be able to read a dataset from a mounted volume or a specified input path.
* The job should perform a data transformation (e.g., estimating π) and write the results to a specified output path.
* The output should be correctly partitioned and saved in a format such as CSV or Parquet.

---

먼저 docker-compose.yml에 볼륨을 마운트하는 부분을 추가하자. (모든 컨테이너에 각각 추가해주었다.)

```yaml
volumes:
  - ${HOME}/docker/volumes/spark/data:${SPARK_HOME}/data
```

다음으로 제출할 Job에 해당하는 python 파일을 작성해야 한다. 과제에서도 언급되어 있는 예제 파일을 이용하기로 하였다. 

시험 삼아 노트북의 로컬 환경에 Java를 설치한 뒤 파이썬 파일을 실행봤는데.. 잘 작동하였다.

```
Pi is roughly 3.148200
```

뒤이어 Parquet 파일로 결과를 저장해 보기도 하였고, 도커에서 실행 중인 Spark cluster에서도 잘 실행되는 것을 확인할 수 있었다.

## 미션 2

> * 미션 2를 진행해 보자. (진행하며 작성 중)

**1. 먼저 요구사항을 분석해야 한다.**

- The application should be able to ingest multiple months or years of data efficiently.
- Handle different file formats such as CSV, Parquet, etc.

링크로 첨부된 사이트에 접속해서 어떤 종류의 데이터가 제공되고 있는지 확인해 보았다.

- Yellow Taxi Trip Records (PARQUET)
- Green Taxi Trip Records (PARQUET)
- For-Hire Vehicle Trip Records (PARQUET)
- High Volume For-Hire Vehicle Trip Records (PARQUET)

**언제부터의 데이터를 수집할 것인지?**

 2019년 2월부터 현재까지는 위 4가지 항목에 대한 데이터를 월 단위로 제공하고 있다.

그 전의 기간에 대해서는 특정 항목이 없는 경우가 있고, 가장 오래된 2009년의 경우 Yellow Taxi Trip Records 데이터만이 존재했다.

`피크 시간 분석`, `기후 조건 분석`을 하기 위해서는 최대한 많은 데이터를 수집하는 것이 유리해 보이면서도 현재의 경향을 잘 분석하기 위해서는 오히려 예전의 데이터가 방해가 될 수도 있다.

동시에 코로나19로 인한 여행 패턴의 변화가 분석 결과에 어떤 영향을 끼칠지도 고민해 봐야겠다는 생각이 들었다.

일단 **2019년 2월부터**의 데이터를 수집하는 것으로 하고, 최대한 확장을 고려해 코드를 작성하여 나중에 필요하다면 그 이전의 데이터도 활용할 수 있도록 하자.

**어떤 데이터를 수집할 것인지?**

- Peak Hours Identification:
  - Identify the peak hours for taxi usage.
- Weather Condition Analysis:
  - Analyze the effect of weather conditions on taxi demand. Use additional datasets if necessary to obtain weather information for the corresponding time periods.

택시 수요에 대한 분석을 진행해야 하므로, `Yellow Taxi`와 `Green Taxi`데이터를 수집하도록 하자. 물론 나머지 항목도 분석에 이용할 수 있겠지만, 데이터 처리에 드는 비용과 시간은 공짜가 아니므로.. 효율적인 방법이 무엇인지 잘 생각해야 한다. 따라서 일단 **택시 여행 기록**만 수집하도록 하자.

이후 Extract 코드를 작성한 결과 잘 작동하였다.

---

**2. 데이터 처리를 어떻게 진행할 것인가?**

과제에는 데이터 정리와 변환에 대한 요구사항도 명시되어 있었다.

- Handle missing values by either removing or imputing them.
- Convert all relevant time fields to a standard timestamp format.
- Filter out records with non-sensical values (e.g., negative trip duration or distance).

결측치와 이상치를 구태여 제거 혹은 대체할 것을 명시한 이유는 무엇일까?

아마도 추출한 데이터를 전처리하여 저장해 놓고, 나중에 분석을 위한 변환을 할 때 항상 정상적인 값들만 다룰 수 있도록 하기 위해서인 것 같다.

---

**코드 작성에 앞서 떠오른 의문을 먼저 정리해 보자.**

* MapReduce를 이용할 때는, HDFS에 데이터를 집어넣어, 입력 데이터가 이미 물리적으로 분산이 되어 있었다.

  * 현재 미션에서는 어떠한가?

  * 물론 Spark와 HDFS를 같이 사용할 수도 있지만, 지금은 단순히 볼륨 마운트를 통해 입력 데이터를 Spark 컨테이너들과 공유한다.
  * pyspark로 코드를 작성할 때 데이터에 대한 분산 처리가 명시적으로 잘 드러나는 것 같지는 않다.
  * 그렇다면 pyspark 라이브러리 혹은 Spark가 알아서 분산 처리를 하는 것일까? 사실 `pandas`를 사용할 때도 `DataFrame` 객체에 대한 연산을 수행하니 멀티 코어를 사용하는 것을 확인했던 적이 있다.
    * 정확히 누가 언제 데이터를 나누고 종합하는 것인지 찾아볼 필요가 있다.

**AI에게서 얻은 답변은 다음과 같다. 물론 학습을 하며 교차 검증을 해야 한다.**

* Spark:

	* 입력 데이터를 자동으로 분할
	* 각 Executor에 작업 할당
	* 필요한 경우 데이터 재분배(shuffle)
	* 결과 취합 및 저장

* pyspark가 따로 Spark의 분산 처리를 도와주는 것은 아닌 듯. 이미 Spark의 기능으로서 잘 구현이 되어 있다.

또한 인터넷 검색을 하다가 RDD(Resillient Distributed Data)라는 개념도 알게 되었다. 다음 문서를 통해 학습하자.

* https://spark.apache.org/docs/latest/rdd-programming-guide.html

---

**3. 코드를 (어떻게 작성할지 생각하며) 작성해보자.**

Extractor와 Loader를 구현했고, 이제 데이터 변환을 담당하는 Transformer를 구현해야 한다.

구현해야 할 사항은 다음과 같다.

* 데이터 정리 및 변환
  * 결측치와 이상치 및 시간 관련 값을 전처리 해야 한다.
* 메트릭 계산
  * 평균 여행 기간, 평균 여행 거리 값을 구해야 한다.
* 분석
  * 피크 시간, 날씨 상태를 분석해야 한다.

위 기능들을, 하나 혹은 여러 개의 Spark Job으로써 실행될 수 있도록 코드를 작성해야 한다.

---

**일단 현재 떠오른 의문들을 정리해 보자.**

* Spark는 RDD에 대한 transformation과 action 연산을 제공한다.
  * 일단 최소한 action이 디스크에 대한 write의 형태로 실행되면 (명시적으로 디스크 사용을 명령한 것과 마찬가지이므로) 메모리 초과가 일어나지 않는 것 같다. 하지만 다른 경우는 어떠한가? 메모리 초과가 발생하는 경우가 있는지?
  * Spark의 장점 중 하나가 바로 최대한 메모리에서 연산을 수행한다는 것(이 표현이 정확한지는 잘 모르겠다..)인데, 결국에는 메모리의 크기를 넘어서는 작업에 대해서는 아무리 파티셔닝을 하더라도 중간 결과를 디스크에 저장해야 할 일이 생길 수 있는 것 아닌가? 그렇다면 Hadoop의 MapReduce를 사용하는 것과 어떤 차이가 있는 것인지?
  * 또한 의도치 않게 OS가 메모리 상의 데이터를 디스크로 Swap Out 할 수 있는 것 아닌가? 이를 방지할 수 있는 기능이 있나?

**AI로부터 빠르게 답을 얻어 보았다. 물론 이후 학습을 진행하며 교차 검증을 해야 한다.**

```
1. 메모리 초과 발생 케이스:

Action 연산 중에서도 메모리 초과가 발생할 수 있습니다. 예를 들어 collect() 액션은 모든 파티션의 데이터를 드라이버로 가져오기 때문에 데이터가 드라이버의 메모리보다 크면 OutOfMemoryError가 발생할 수 있습니다.
특히 groupByKey(), join() 같은 transformation에서 셔플링이 발생하는 경우, 특정 파티션에 데이터가 쏠리면 익스큐터의 메모리 초과가 발생할 수 있습니다.

2. Spark vs Hadoop MapReduce의 메모리 관리:

Spark도 메모리를 초과하는 데이터는 디스크에 저장합니다. 하지만 주요한 차이점이 있습니다:

Spark는 중간 결과를 메모리에 캐시할 수 있어서, 반복 연산이 필요한 경우 큰 성능 이점이 있습니다.
MapReduce는 모든 단계에서 강제로 디스크 I/O가 발생하지만, Spark는 메모리가 충분하면 디스크 I/O 없이 파이프라인으로 처리할 수 있습니다.
Spark의 RDD 리니지(lineage)를 통해 특정 파티션만 재계산이 가능해서, 실패 복구도 더 효율적입니다.

3. 메모리 Swap 관리:

Spark는 OS의 메모리 스왑을 제어하기 위한 몇 가지 설정을 제공합니다:

spark.executor.memoryOverhead: JVM 외부 메모리 할당량을 조절할 수 있습니다.
spark.memory.offHeap.enabled와 spark.memory.offHeap.size: Off-heap 메모리 사용을 설정할 수 있습니다.

시스템 레벨에서는:

Linux의 경우 vm.swappiness 파라미터를 조절하여 스왑 발생을 최소화할 수 있습니다.
컨테이너 환경(예: Kubernetes)에서는 메모리 제한을 설정하여 스왑을 방지할 수 있습니다.

4. 실제 운영 환경에서는 이러한 특성을 고려하여:

적절한 파티셔닝 전략 수립
메모리 사용량이 큰 연산(예: groupByKey 대신 reduceByKey 사용)의 최적화
캐시할 RDD의 신중한 선택
적절한 메모리 설정

등을 통해 성능을 최적화하는 것이 중요합니다.
```

또한 애초에 Spark가 어떻게 작동하는지를 알아야 할 필요가 있다.

스스로 Job이라는 단어를 사용하면서도 아직은 그게 무엇인지 잘 모르겠다. 이를 Spark가 어떻게 처리하는지도..

미션을 진행하면서 학습하도록 하자.

---

**앞서 적은 구현 필요 사항들을 어떻게 구현할지 생각해 보았다.**

* 데이터 정리 및 변환
  * 다른 처리와 독립적인 Spark Job으로 실행한다. 물론 결과를 디스크에 저장해야 한다.
* 메트릭 계산
  * 분석 단계에서 수행한다.
  * 어차피 데이터 분석 시 다양한 집계 연산이 호출될 수 있는데 평균 계산을 굳이 미리 혹은 따로 수행할 필요가 없어 보인다.
  * 물론 분석 단계와 분리하고 '데이터 정리 및 변환'과 동시에 메트릭 계산을 할 수도 있긴 함.

