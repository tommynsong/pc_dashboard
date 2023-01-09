'''Serves API endpoints for front to back end interactions'''
import os
import time
import logging
import json
import requests
import psycopg2
import psycopg2.extras
from flask import Flask, request


def db_connect():
    conn = None
    try:
        conn = psycopg2.connect(
            host='postgres-edw',
            database=os.environ['POSTGRES_DB'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD']
        )
    except (Exception, psycopg2.DatabaseError) as msg:
        print("Error in db_connect function:")
        print(msg)
        return (1)
    return conn


def init_settings():
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS reporting.settings (
            id SERIAL PRIMARY KEY,
            type VARCHAR(16),
            apiurl VARCHAR(36),
            apikey VARCHAR(128),
            apisecret VARCHAR(128)
        );
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(create_table_sql)
    return 0


connection = db_connect()
while connection == 1:
    time.sleep(5)
    connection = db_connect()

init_settings()
app = Flask(__name__)


@app.post("/api/prismastatus")
def prisma_status():
    data = json.loads(request.get_json())
    pc_url = (data["apiurl"] + '/login')
    payload = {
        "username": data["apikey"],
        "password": data["apisecret"],
    }
    headers = {"content-type": "application/json; charset=UTF-8"}
    response = requests.post(pc_url, json=payload, headers=headers, timeout=10)
    if response.status_code == 200:
        return ({"id": 1, "message": "Successful Connection"}, 200)
    else:
        return ({"id": 1, "message": "Unsuccessful Connection"}, response.status_code)


@app.post("/api/etljobs")
def update_etl_jobs():
    data = json.loads(request.get_json())
    conn_name = data["conn_name"]
    conn_since = data["conn_since"]
    next_run = data["next_run"]
    last_run = data["last_run"]
    elapsed = data["elapsed"]
    retention = data["retention"]
    int_time = data["int_time"]
    sql = """
        INSERT INTO reporting.etl_jobs (conn_name, conn_since, last_run,
        next_run, elapsed, retention, int_time) VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (conn_name, conn_since,
                           last_run, next_run, elapsed,
                           retention, int_time))
    return ({"id": 1, "message": "Connection added."}, 201)


@app.post("/api/prismasettings")
def update_settings():
    data = json.loads(request.get_json())
    pc_url = data["apiurl"]
    pc_key = data["apikey"]
    pc_secret = data["apisecret"]
    add_pc_settings = """
        INSERT INTO reporting.settings (type, apiurl, apikey, apisecret)
        VALUES (%s, %s, %s, %s) RETURNING id;
    """
    update_pc_settings = """
        UPDATE reporting.settings
        SET apiurl = %s, apikey = %s, apisecret = %s
        WHERE type = 'prisma';
    """
    get_settings_sql = """
        SELECT apiurl, apikey, apisecret
        FROM reporting.settings
        WHERE type = 'prisma'
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(get_settings_sql)
            row = cursor.fetchone()
            if row:
                cursor.execute(update_pc_settings, (pc_url, pc_key, pc_secret))
            else:
                cursor.execute(add_pc_settings, ('prisma',
                                                 pc_url, pc_key, pc_secret,))
    return ({"id": 1, "message": "Room created."}, 201)


@app.get("/api/prismasettings")
def get_settings():
    with connection:
        with connection.cursor() as cursor:
            get_settings_sql = """
                SELECT apiurl, apikey, apisecret
                FROM reporting.settings
                WHERE type = 'prisma'
            """
            cursor.execute(get_settings_sql)
            row = cursor.fetchone()
            if row:
                return ({"apiurl": row[0], "apikey": row[1], "apisecret": row[2]}, 201)
            else:
                return ('', 204)


@app.get("/api/etljobs")
def get_etl_jobs():
    args = request.args
    etl_name = args.get('etl_name')
    with connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            if etl_name is None:
                #sql = "SELECT jsonb_agg(to_jsonb(l)) FROM reporting.etl_jobs l"
                sql = "SELECT * FROM reporting.etl_jobs"
            else:
                sql = """SELECT * FROM reporting.etl_jobs
                    WHERE conn_name = \'""" + etl_name + "\'"
                # sql = """SELECT jsonb_agg(to_jsonb(l)) FROM reporting.etl_jobs l
                #    WHERE conn_name = \'""" + etl_name + "\'"
            cursor.execute(sql)
            records = cursor.fetchall()
            if records:
                return records, 201
            else:
                return '', 204


if __name__ == "__main__":
    from waitress import serve
    logger = logging.getLogger('waitress')
    logger.setLevel(logging.DEBUG)
    serve(app, host="0.0.0.0", port=5050)
