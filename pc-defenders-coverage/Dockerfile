FROM python:3.9.16
COPY ./requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY ./src/. /app
LABEL org.opencontainers.image.authors="nospam.joe@gmail.com"
LABEL org.opencontainers.image.source="https://github.com/jtb75/pc_dashboard/tree/main/pc-defenders-coverage"
LABEL org.opencontainers.image.vendor="jtb75"
ENTRYPOINT [ "python" ]
CMD ["app.py" ]