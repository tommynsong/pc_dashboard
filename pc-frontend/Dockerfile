FROM python:3.9.16
COPY ./requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt
COPY ./src/. /app
LABEL org.opencontainers.image.authors="nospam.joe@gmail.com"
LABEL org.opencontainers.image.source="https://github.com/jtb75/pc_dashboard/tree/main/pc-frontend"
LABEL org.opencontainers.image.vendor="jtb75"
ENTRYPOINT [ "gunicorn" ]
CMD ["frontend:server", "-b", "0.0.0.0:8050" ]