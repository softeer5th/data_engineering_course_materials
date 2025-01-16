FROM quay.io/jupyter/base-notebook:latest

# 작업 디렉터리 설정
WORKDIR /app

COPY requirements.txt .

# Python 패키지 설치
RUN pip install -r requirements.txt

# 프로젝트 파일 복사
COPY . .

# juptyer notebook 실행
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]