# W3M3 - Word Count using MapReduce

## 개요

이 문서는 Hadoop 3.4.1 멀티 노드 클러스터에서 E-book의 단어 수를 MapReduce 과정을 통해 계산하는 방법에 대한 단계별 지침을 제공합니다.

---

## 1. 파일 올리기

### 1. E-Book txt 파일 받기
https://www.gutenberg.org/ 사이트에서 쉽게 e-book 파일을 다운로드 받을 수 있습니다.

### 2. 로컬 파일을 Container에 올리기

`ebook.txt`, `mapper.py`, `reducer.py`를 모두 컨테이너에 넣어줍니다. :

    docker cp <로컬 파일 경로>  <컨테이너 이름>:<컨테이너 내 파일 경로>

### 3. 컨테이너에 접속
실행 중인 컨테이너에 접속하려면 다음 명령어를 사용합니다:

    docker exec -it <컨테이너 이름> /bin/bash

### 4. HDFS에 e-book.txt 올리기

ebook은 텍스트 데이터로서 하둡으로 올려야하기 때문에 하둡에 폴더를 만들어주고, 그 공간에 파일을 올려줍니다. :

    hdfs dfs -mkdir -p <하둡에 저장할 폴더 경로>
    hdfs dfs -put /tmp/ebook.txt <하둡에 저장할 폴더 경로>

### 5. 업로드 확인
파일이 원하는 경로에 올라갔는지 확인합니다. :

    hdfs dfs -ls /input

ebook.txt가 출력된다면 정상적으로 실행된 것입니다.