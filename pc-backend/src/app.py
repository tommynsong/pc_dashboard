'''Serves API endpoints for front to back end interactions'''
import os
import time
import logging
import json
import requests
import psycopg2
import psycopg2.extras
from flask import Flask, request
from waitress import serve

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


def db_connect():
    '''
    Set up database connection
    Return connection
    '''
    logging.info('Connecting to Database')
    conn = None
    try:
        conn = psycopg2.connect(
            host='postgres-edw',
            database=os.environ['POSTGRES_DB'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD']
        )
    except psycopg2.OperationalError as error:
        logging.error(error)
        return (1)
    return conn


def init_settings():
    '''
    Retrieve DB conn from db_connect()
    Configuration DB tables, if not created
    Close DB conn
    '''
    connection = db_connect()
    while connection == 1:
        time.sleep(5)
        connection = db_connect()
    logging.info('Initializing Database Tables')
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
            try:
                cursor.execute(create_table_sql)
            except psycopg2.OperationalError as error:
                logging.error(error)
                connection.close()
                return 1
    connection.close()
    return 0


init_settings()
app = Flask(__name__)


@app.post("/api/prismastatus")
def prisma_status():
    '''
    Tests connectivity and credentials to Prisma Cloud API
    Receives Prisma Cloud API URL, API Key, and API Secret
    Attempts to login using supplied credentials
    Returns 200 on success
    '''
    logging.info('Checking Prisma Cloud connectivity and credentials')
    data = json.loads(request.get_json())
    pc_url = (data["apiurl"] + '/login')
    payload = {
        "username": data["apikey"],
        "password": data["apisecret"],
    }
    headers = {"content-type": "application/json; charset=UTF-8"}
    try:
        response = requests.post(pc_url, json=payload,
                                 headers=headers, timeout=10)
        if response.status_code == 200:
            logging.info(
                'Successfully obtained Prisma Cloud authentication token')
            return ({"message": "Successful Connection"}, response.status_code)
        logging.info('Unable to obtain Prisma Cloud authentication token')
        return ({"message": "Unsuccessful Connection"}, response.status_code)
    except requests.exceptions.RequestException as error:
        logging.error(error)
        return ({"message": error}, 500)


@app.post("/api/etljobs")
def update_etl_jobs():
    '''
    Creates DB connection then registers new etl jobs
    Receives and inputs etl jobs attributes and returns 201 if created successfully
    Closes DB connection when finished
    '''
    connection = db_connect()
    while connection == 1:
        time.sleep(5)
        connection = db_connect()
    logging.info('Registering etl job in DB')
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
            try:
                cursor.execute(sql, (conn_name, conn_since,
                                     last_run, next_run, elapsed,
                                     retention, int_time))
            except psycopg2.OperationalError as error:
                logging.error(error)
                connection.close()
                return ({"message": error}, 500)
    logging.info('Successfully added etl job')
    connection.close()
    return ({"message": "Connection added."}, 201)


@app.post("/api/prismasettings")
def update_settings():
    '''
    Updates the Prisma Cloud settings stored in the dettings DB table
    Receives Prisma Cloud API URL, API Key, and API Secret
    '''
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
    connection = db_connect()
    while connection == 1:
        time.sleep(5)
        connection = db_connect()
    with connection:
        with connection.cursor() as cursor:
            logging.info('Getting Prisma Cloud settings from DB')
            try:
                cursor.execute(get_settings_sql)
                row = cursor.fetchone()
                if row:
                    logging.info('Updating current Prisma Cloud settings')
                    try:
                        cursor.execute(update_pc_settings,
                                       (pc_url, pc_key, pc_secret))
                    except psycopg2.OperationalError as error:
                        logging.error(error)
                        connection.close()
                        return ({"message": error}, 500)
                else:
                    logging.info('Add new Prisma Cloud settings')
                    try:
                        cursor.execute(add_pc_settings, ('prisma',
                                                         pc_url, pc_key, pc_secret,))
                    except psycopg2.OperationalError as error:
                        logging.error(error)
                        connection.close()
                        return ({"message": error}, 500)
            except psycopg2.OperationalError as error:
                logging.error(error)
                connection.close()
                return ({"message": error}, 500)
    logging.info('Successfully entered Prisma Cloud settings')
    connection.close()
    return ({"message": "Prisma Cloud settings successful."}, 201)


@app.get("/api/prismasettings")
def get_settings():
    '''
    Returns Prisma Cloud Settings from the DB
    '''
    connection = db_connect()
    while connection == 1:
        time.sleep(5)
        connection = db_connect()
    logging.info('Retrieving Prisma Cloud Settings from DB')
    with connection:
        with connection.cursor() as cursor:
            get_settings_sql = """
                SELECT apiurl, apikey, apisecret
                FROM reporting.settings
                WHERE type = 'prisma'
            """
            try:
                cursor.execute(get_settings_sql)
            except psycopg2.OperationalError as error:
                logging.error(error)
                return ({"message": error}, 500)
            row = cursor.fetchone()
            if row:
                logger.info('Prisma Cloud settings found and returned')
                return ({"apiurl": row[0], "apikey": row[1], "apisecret": row[2]}, 201)
            logger.info('No Prisma Cloud settings found in DB')
            return ('', 204)


@app.get("/api/etljobs")
def get_etl_jobs():
    '''
    Get etl jobs from DB and return
    '''
    args = request.args
    etl_name = args.get('etl_name')
    connection = db_connect()
    while connection == 1:
        time.sleep(5)
        connection = db_connect()
    logging.info('Getting etl job listing matching etl name from DB')
    with connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            if etl_name is None:
                sql = "SELECT * FROM reporting.etl_jobs"
            else:
                sql = """SELECT * FROM reporting.etl_jobs
                    WHERE conn_name = \'""" + etl_name + "\'"
            try:
                cursor.execute(sql)
                records = cursor.fetchall()
            except psycopg2.OperationalError as error:
                logging.error(error)
                connection.close()
                return ({"message": error}, 500)
            if records:
                logging.info('Found and returning registered etl jobs')
                return records, 201
            logging.info('No registered etl jobs found')
            return '', 204


if __name__ == "__main__":
    logger = logging.getLogger('waitress')
    logger.setLevel(logging.DEBUG)
    serve(app, host="0.0.0.0", port=5050)
