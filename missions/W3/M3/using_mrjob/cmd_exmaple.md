
아래 명령어 시도 중인데 아직 작동 안 함....

```bash
python3 mapred.py \
-r hadoop \
hdfs:///user/root/input/book.txt \
--output-dir hdfs:///user/root/output \
--python-bin python3 \
--setup 'pip install mrjob' \
--hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar
```