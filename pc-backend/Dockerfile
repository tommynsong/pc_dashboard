FROM python:3.9.16
COPY ./requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY ./src/. /app
LABEL org.opencontainers.image.authors="spamblackhole.tommy@gmail.com"
LABEL org.opencontainers.image.source="https://github.com/tommynsong/pc_dashboard/tree/main/pc-backend"
LABEL org.opencontainers.image.vendor="focer"
ENTRYPOINT [ "python" ]
CMD ["app.py" ]
