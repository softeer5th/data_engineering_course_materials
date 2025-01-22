입력 파일 HDFS에 업로드

```bash
# HDFS에 input 디렉토리 생성
hadoop fs -mkdir -p /user/root/input
# book.txt 파일을 HDFS에 업로드
hadoop fs -put book.txt /user/root/input/
```

필요 시 지난 output 제거

```bash
hadoop fs -rm -r -f /user/root/output
```

맵리듀스 실행

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -file mapper.py -mapper 'python3 mapper.py' \
    -file reducer.py -reducer 'python3 reducer.py' \
    -input /user/root/input/book.txt \
    -output /user/root/output
```