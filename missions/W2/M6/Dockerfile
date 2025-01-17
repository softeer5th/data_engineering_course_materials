FROM python:3.10.16

RUN apt-get update && apt-get install -y build-essential

RUN pip install pandas==2.2.3
RUN pip install matplotlib==3.10.0
RUN pip install wordcloud==1.9.4
RUN pip install jupyter==1.1.1
# 작업 디렉토리를 /app으로 설정
WORKDIR /M6

# hello.py와 userdata.txt를 컨테이너에 복사
COPY /src /M6/src
COPY hello.py /M6/

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root"]