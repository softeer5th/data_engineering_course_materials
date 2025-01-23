FROM jupyter_image_amd:w1

WORKDIR /app

COPY ./W2 /app/notebook/w2

EXPOSE 8888

CMD ["jupyter", "notebook", "--allow-root", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
