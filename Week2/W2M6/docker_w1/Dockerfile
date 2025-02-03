# Base image
# Python 3.9가 설치된 슬림 버전의 이미지를 기반으로 함
FROM python:3.9-slim

# 작업 디렉토리를 설정
WORKDIR /Mission_app

# 호스트 머신의 파일이나 디렉토리를 컨테이너의 파일 시스템으로 복사
COPY Week1 /Mission_app/app/Week1

# 필요한 패키지 설치
RUN pip install --no-cache-dir jupyterlab

# RUN 명령어는 컨테이너 내에서 명령어를 실행. 여기서는 시스템 종속성을 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    sqlite3 \
    libsqlite3-dev \
    && apt-get clean

# Install required Python libraries
RUN pip install --no-cache-dir \
    pandas \
    requests \
    beautifulsoup4 \
    tabulate \
    matplotlib \
    seaborn \
    scikit-learn

# Additional dependencies based on scripts
RUN pip install --no-cache-dir configparser

# EXPOSE 명령어는 컨테이너가 수신할 포트를 지정
EXPOSE 8888

# Set default command (can be adjusted to run specific scripts or a notebook server)
CMD ["jupyter-lab", "--ip=0.0.0.0", "--port=8888", "--allow-root"]

