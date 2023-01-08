FROM python:3.9.16
COPY ./requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt
COPY ./src/. /app
ENTRYPOINT [ "gunicorn" ]
CMD ["frontend:server", "-b", "0.0.0.0:8050" ]