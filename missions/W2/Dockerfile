# docker build -f Dockerfile -t jupyter ..
FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install jupyterlab pandas matplotlib beautifulsoup4 wordcloud 

EXPOSE 8888

RUN mkdir -p /root/.jupyter/ && \
    echo 'c.ServerApp.password = "argon2:$argon2id$v=19$m=10240,t=10,p=8$j5lAV2twSXfH1hygBV14xA$bCHDtGS/opm6wiOvu/1fjUDn4uthYRKiOk86reRAtvc"' > /root/.jupyter/jupyter_server_config.py

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
