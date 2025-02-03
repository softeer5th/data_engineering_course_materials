# Base image: Spark with Jupyter
FROM jupyter/pyspark-notebook:latest

# 작업 디렉토리 설정
WORKDIR /app

# 필요한 패키지 설치
RUN pip install pandas matplotlib seaborn

# 데이터 디렉토리 생성
RUN mkdir -p /app/data/output

# 현재 디렉토리 내 모든 파일을 컨테이너에 복사
COPY . .

USER root
RUN chmod +x /app/run-analysis.sh
RUN mkdir -p /usr/local/spark/logs && chmod -R 777 /usr/local/spark/logs

RUN chmod -R 777 /app/data/output


USER jovyan

# Jupyter Notebook을 실행하기 전에 스크립트 실행 (Spark 분석 실행)
CMD ["bash", "start-spark.sh"]