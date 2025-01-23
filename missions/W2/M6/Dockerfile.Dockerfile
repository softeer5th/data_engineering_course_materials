FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && pip install --no-cache-dir \
    jupyter notebook

COPY ./notebooks /app/notebook

EXPOSE 8888

CMD ["jupyter", "notebook", "--allow-root", "--ip=0.0.0.0", "--port=8888", "--no-browser"]